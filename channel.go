package client

import (
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

type handler func(channel *remoteChannel, msg *Proto)

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
	rcvDone   chan *Proto

	cmdReactors map[Command]handler
}

func (ch *remoteChannel) registerHandler() {
	ch.cmdReactors[CMD_CREATE_OK] = func(channel *remoteChannel, msg *Proto) {
		channel.onCreateOk(msg)
	}
	ch.cmdReactors[CMD_SND_OK] = func(channel *remoteChannel, msg *Proto) {
		channel.onSndOk(msg)
	}
	ch.cmdReactors[CMD_RCV_OK] = func(channel *remoteChannel, msg *Proto) {
		channel.onRcvOk(msg)
	}
	ch.cmdReactors[CMD_SND_BLOCK] = func(channel *remoteChannel, msg *Proto) {
		channel.onSndBlock(msg)
	}
	ch.cmdReactors[CMD_RCV_BLOCK] = func(channel *remoteChannel, msg *Proto) {
		channel.onRcvBlock(msg)
	}
	ch.cmdReactors[CMD_CLOSED] = func(channel *remoteChannel, msg *Proto) {
		channel.onChClosed(msg)
	}
	ch.cmdReactors[CMD_SND_WAKEUP] = func(channel *remoteChannel, msg *Proto) {
		channel.onChSndWakeUp(msg)
	}
	ch.cmdReactors[CMD_RCV_WAKEUP] = func(channel *remoteChannel, msg *Proto) {
		channel.onChRcvWakeUp(msg)
	}

	// error handing
	ch.cmdReactors[CMD_ERRCMD] = func(channel *remoteChannel, msg *Proto) {
		logger.Errorf("error occur, cmd:%d, ch:%s", msg.Cmd, msg.Name)
	}
	ch.cmdReactors[CMD_ERRNOTFOUND] = ch.cmdReactors[CMD_ERRCMD]
	ch.cmdReactors[CMD_ERRPROTO] = ch.cmdReactors[CMD_ERRCMD]
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
		rcvDone:     make(chan *Proto),
		sndReady:    make(chan struct{}, 1),
		rcvReady:    make(chan struct{}, 1),
		l:           sync.Mutex{},
		cmdReactors: map[Command]handler{},
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

	ch.netSession.Send(&Proto{
		Cmd:  CMD_CLOSE,
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
	ch.netSession.Send(&Proto{
		Cmd:  CMD_IN,
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
	ch.netSession.Send(&Proto{
		Cmd:  CMD_OUT,
		Name: ch.name,
	})

	msg := <-ch.rcvDone
	if msg.Cmd == CMD_RCV_OK || msg.Cmd == CMD_RCV_WAKEUP {
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
	msg, ok := ev.Message().(*Proto)
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
func (ch *remoteChannel) onCreateOk(msg *Proto) {
	ch.l.Lock()
	defer ch.l.Unlock()

	ch.status = NORMAL
	ch.connected <- struct{}{}
}

// FIXME after ch closed
func (ch *remoteChannel) onSndOk(msg *Proto) {
	ch.l.Lock()
	defer ch.l.Unlock()

	ch.sndStatus = NORMAL
	ch.sndDone <- struct{}{}
}

// FIXME after ch closed
func (ch *remoteChannel) onSndBlock(msg *Proto) {
	ch.l.Lock()
	defer ch.l.Unlock()

	ch.sndStatus = BLOCK_IN
}

// FIXME after ch closed
func (ch *remoteChannel) onRcvOk(msg *Proto) {
	ch.l.Lock()
	defer ch.l.Unlock()

	ch.rcvStatus = NORMAL
	ch.rcvDone <- msg
}

// FIXME after ch closed
func (ch *remoteChannel) onRcvBlock(msg *Proto) {
	ch.l.Lock()
	defer ch.l.Unlock()

	ch.rcvStatus = BLOCK_OUT
}

func (ch *remoteChannel) onChClosed(msg *Proto) {
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

func (ch *remoteChannel) onChSndWakeUp(msg *Proto) {
	ch.sndDone <- struct{}{}
}

func (ch *remoteChannel) onChRcvWakeUp(msg *Proto) {
	ch.rcvDone <- msg
}

func (ch *remoteChannel) onConnected(s gnet.NetSession) {
	ch.netSession = s
	ch.status = NET_CREATE

	ch.netSession.Send(&Proto{
		Cmd:  CMD_NEWORCREAT,
		Name: ch.name,
		Data: []byte(strconv.Itoa(ch.cap)),
	})
}

func (ch *remoteChannel) onClose(s gnet.NetSession) {

}
