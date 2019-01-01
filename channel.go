package remote_channel

import (
	"github.com/MaxnSter/remote_channel/cmd"
	"reflect"
	"sync"

	"github.com/MaxnSter/gnet"
	"github.com/MaxnSter/gnet/codec"
	"github.com/MaxnSter/gnet/logger"
	"github.com/MaxnSter/gnet/message_pack/message_meta"
	"strconv"

	_ "github.com/MaxnSter/gnet/codec/codec_msgpack"
	_ "github.com/MaxnSter/gnet/message_pack/pack/pack_type_length_value"
	_ "github.com/MaxnSter/gnet/net/tcp"
	_ "github.com/MaxnSter/gnet/worker_pool/worker_session_race_other"
)

var coder codec.Coder

func init() {
	coder = codec.MustGetCoder("msgpack")
}

type handler func(channel *remoteChannel, msg *cmd.Proto)

type remoteChannel struct {
	cap  int
	name string

	sndStatus ChStatus
	rcvStatus ChStatus
	status    ChStatus // guard by l
	l         sync.Mutex

	addr       string
	meta       *message_meta.MessageMeta
	netClient  gnet.NetClient
	netSession gnet.NetSession

	connected chan struct{}
	sndReady  chan struct{}
	rcvReady  chan struct{}
	sndDone   chan struct{}
	rcvDone   chan *cmd.Proto

	cmdReactors map[cmd.Command]handler
}

func (ch *remoteChannel) registerHandler() {
	ch.cmdReactors[cmd.CMD_CREATE_OK] = func(channel *remoteChannel, msg *cmd.Proto) {
		channel.onCreateOk(msg)
	}
	ch.cmdReactors[cmd.CMD_SND_OK] = func(channel *remoteChannel, msg *cmd.Proto) {
		channel.onSndOk(msg)
	}
	ch.cmdReactors[cmd.CMD_RCV_OK] = func(channel *remoteChannel, msg *cmd.Proto) {
		channel.onRcvOk(msg)
	}
	ch.cmdReactors[cmd.CMD_SND_BLOCK] = func(channel *remoteChannel, msg *cmd.Proto) {
		channel.onSndBlock(msg)
	}
	ch.cmdReactors[cmd.CMD_RCV_BLOCK] = func(channel *remoteChannel, msg *cmd.Proto) {
		channel.onRcvBlock(msg)
	}
	ch.cmdReactors[cmd.CMD_CLOSED] = func(channel *remoteChannel, msg *cmd.Proto) {
		channel.onChClosed(msg)
	}
	ch.cmdReactors[cmd.CMD_SND_WAKEUP] = func(channel *remoteChannel, msg *cmd.Proto) {
		channel.onChSndWakeUp(msg)
	}
	ch.cmdReactors[cmd.CMD_RCV_WAKEUP] = func(channel *remoteChannel, msg *cmd.Proto) {
		channel.onChRcvWakeUp(msg)
	}

	// error handing
	ch.cmdReactors[cmd.CMD_ERRCMD] = func(channel *remoteChannel, msg *cmd.Proto) {
		logger.Errorf("error occur, cmd:%d, ch:%s", msg.Cmd, msg.Name)
	}
	ch.cmdReactors[cmd.CMD_ERRNOTFOUND] = ch.cmdReactors[cmd.CMD_ERRCMD]
	ch.cmdReactors[cmd.CMD_ERRPROTO] = ch.cmdReactors[cmd.CMD_ERRCMD]
}

func (ch *remoteChannel) makeHandler() {

}

func NewRemoteChannel(name string, dataType reflect.Type, cap int) *remoteChannel {
	if dataType.Kind() == reflect.Ptr {
		dataType = dataType.Elem()
	}

	r := &remoteChannel{
		cap:         cap,
		name:        name,
		status:      NORMAL,
		meta:        message_meta.NewMessageMeta(0, dataType),
		connected:   make(chan struct{}),
		sndDone:     make(chan struct{}),
		rcvDone:     make(chan *cmd.Proto),
		sndReady:    make(chan struct{}, 1),
		rcvReady:    make(chan struct{}, 1),
		l:           sync.Mutex{},
		cmdReactors: map[cmd.Command]handler{},
	}

	r.registerHandler()
	return r
}

func (ch *remoteChannel) Make(addr string) *remoteChannel {
	m := gnet.NewModule(gnet.WithPool("poolRaceOther"), gnet.WithPacker("tlv"),
		gnet.WithCoder("msgpack"))

	o := gnet.NewOperator(ch.onMessage)
	o.SetOnClose(ch.onClose)
	o.SetOnConnected(ch.onConnected)

	ch.netClient = gnet.NewNetClient("tcp", "remote_channel", m, o)
	ch.addr = addr
	go ch.netClient.Connect(addr)

	<-ch.connected
	return ch
}

func (ch *remoteChannel) Close() {
	ch.l.Lock()
	if ch.status == CLOSED {
		ch.l.Unlock()
		return
	}

	ch.netSession.Send(&cmd.Proto{
		Cmd:  cmd.CMD_CLOSE,
		Name: ch.name,
	})
}

func (ch *remoteChannel) In(data interface{}) {
	var dataEncoded []byte
	var err error

	if dataEncoded, err = coder.Encode(data); err != nil {
		logger.Errorln("encode data error")
		return
	}

	ch.l.Lock()
	if ch.status == CLOSED {
		ch.l.Unlock()
		panic("send on a closed ch")
	}

	ch.sndReady <- struct{}{}
	ch.sndStatus = NET_IN
	ch.l.Unlock()
	ch.netSession.Send(&cmd.Proto{
		Cmd:  cmd.CMD_IN,
		Name: ch.name,
		Data: dataEncoded,
	})

	<-ch.sndDone
	<-ch.sndReady

}

func (ch *remoteChannel) Out() (data interface{}, ok bool) {
	ch.l.Lock()
	if ch.status == CLOSED {
		ch.l.Unlock()
		return nil, false
	}

	ch.rcvReady <- struct{}{}
	ch.rcvStatus = NET_OUT
	ch.l.Unlock()
	ch.netSession.Send(&cmd.Proto{
		Cmd:  cmd.CMD_OUT,
		Name: ch.name,
	})

	msg := <-ch.rcvDone
	if msg.Cmd == cmd.CMD_RCV_OK || msg.Cmd == cmd.CMD_RCV_WAKEUP {
		newType := ch.meta.NewType()
		if err := coder.Decode(msg.Data, newType); err != nil {
			logger.Errorln("decode data error")
		}

		data, ok = newType, true
	} else {
		data, ok = nil, false
	}

	<-ch.rcvReady
	return
}

func (ch *remoteChannel) onMessage(ev gnet.Event) {
	msg, ok := ev.Message().(*cmd.Proto)
	if !ok {
		logger.Errorln("msg type assert error")
		return
	}

	if _, ok := ch.cmdReactors[msg.Cmd]; !ok {
		logger.Errorf("unregister handler for cmd:%s", msg.Cmd)
		return
	}

	ch.cmdReactors[msg.Cmd](ch, msg)
}

// FIXME after ch closed
func (ch *remoteChannel) onCreateOk(msg *cmd.Proto) {
	ch.l.Lock()
	defer ch.l.Unlock()

	ch.status = NORMAL
	ch.connected <- struct{}{}
}

// FIXME after ch closed
func (ch *remoteChannel) onSndOk(msg *cmd.Proto) {
	ch.l.Lock()
	defer ch.l.Unlock()

	ch.sndStatus = NORMAL
	ch.sndDone <- struct{}{}
}

// FIXME after ch closed
func (ch *remoteChannel) onSndBlock(msg *cmd.Proto) {
	ch.l.Lock()
	defer ch.l.Unlock()

	ch.sndStatus = BLOCK_IN
}

// FIXME after ch closed
func (ch *remoteChannel) onRcvOk(msg *cmd.Proto) {
	ch.l.Lock()
	defer ch.l.Unlock()

	ch.rcvStatus = NORMAL
	ch.rcvDone <- msg
}

// FIXME after ch closed
func (ch *remoteChannel) onRcvBlock(msg *cmd.Proto) {
	ch.l.Lock()
	defer ch.l.Unlock()

	ch.rcvStatus = BLOCK_OUT
}

func (ch *remoteChannel) onChClosed(msg *cmd.Proto) {
	ch.l.Lock()
	defer func() {
		ch.status = CLOSED
		ch.l.Unlock()
	}()

	if ch.status == NET_CREATE {
		ch.connected <- struct{}{}
	}
	if ch.sndStatus == NET_IN || ch.sndStatus == BLOCK_IN {
		ch.sndDone <- struct{}{}
	}
	if ch.rcvStatus == NET_OUT || ch.rcvStatus == BLOCK_OUT {
		ch.rcvDone <- msg
	}
}

func (ch *remoteChannel) onChSndWakeUp(msg *cmd.Proto) {
	ch.sndDone <- struct{}{}
}

func (ch *remoteChannel) onChRcvWakeUp(msg *cmd.Proto) {
	ch.rcvDone <- msg
}

func (ch *remoteChannel) onConnected(s gnet.NetSession) {
	ch.netSession = s
	ch.status = NET_CREATE

	ch.netSession.Send(&cmd.Proto{
		Cmd:  cmd.CMD_NEWORCREAT,
		Name: ch.name,
		Data: []byte(strconv.Itoa(ch.cap)),
	})
}

func (ch *remoteChannel) onClose(s gnet.NetSession) {

}
