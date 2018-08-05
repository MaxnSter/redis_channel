package redis_channel

import (
	"reflect"
	"sync"

	"github.com/MaxnSter/gnet"
	"github.com/MaxnSter/gnet/codec"
	"github.com/MaxnSter/gnet/logger"
	"github.com/MaxnSter/gnet/message_pack/message_meta"
	"strconv"

	_ "github.com/MaxnSter/gnet/net/tcp"
	_ "github.com/MaxnSter/gnet/worker_pool/worker_session_race_other"
	_ "github.com/MaxnSter/gnet/codec/codec_msgpack"
	_ "github.com/MaxnSter/gnet/message_pack/pack/pack_type_length_value"
)

var coder codec.Coder

func init() {
	coder = codec.MustGetCoder("msgpack")
}

type redisChannel struct {
	cap  int
	name string

	status ChStatus // guard by l
	l      sync.Mutex

	addr       string
	meta       *message_meta.MessageMeta
	netClient  gnet.NetClient
	netSession gnet.NetSession

	connected chan struct{}
	sndReady  chan struct{}
	rcvReady  chan struct{}
	sndDone   chan struct{}
	rcvDone   chan *Proto
}

func NewRedisChannel(cap int, name string, dataType reflect.Type) *redisChannel {
	if dataType.Kind() == reflect.Ptr {
		dataType = dataType.Elem()
	}

	r := &redisChannel{
		cap:       cap,
		name:      name,
		status:    NORMAL,
		meta:      message_meta.NewMessageMeta(0, dataType),
		connected: make(chan struct{}),
		sndDone:   make(chan struct{}),
		rcvDone:   make(chan *Proto),
		sndReady:  make(chan struct{}, 1),
		rcvReady:  make(chan struct{}, 1),
		l:         sync.Mutex{},
	}

	return r
}

func (ch *redisChannel) Make(addr string) *redisChannel {
	m := gnet.NewModule(gnet.WithPool("poolRaceOther"), gnet.WithPacker("tlv"),
		gnet.WithCoder("msgpack"))

	o := gnet.NewOperator(ch.onMessage)
	o.SetOnClose(ch.onClose)
	o.SetOnConnected(ch.onConnected)

	ch.netClient = gnet.NewNetClient("tcp", "redis_channel", m, o)
	ch.addr = addr
	go ch.netClient.Connect(addr)

	<-ch.connected
	return ch
}

func (ch *redisChannel) In(data interface{}) {
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
	ch.status = NET_IN
	ch.l.Unlock()
	ch.netSession.Send(&Proto{
		Cmd:  CMD_IN,
		Name: ch.name,
		Data: dataEncoded,
	})

	<-ch.sndDone
	<-ch.sndReady

}

func (ch *redisChannel) Out() (data interface{}, ok bool) {
	ch.l.Lock()
	if ch.status == CLOSED {
		ch.l.Unlock()
		return nil, false
	}

	ch.rcvReady <- struct{}{}
	ch.status = NET_OUT
	ch.l.Unlock()
	ch.netSession.Send(&Proto{
		Cmd:  CMD_OUT,
		Name: ch.name,
	})

	msg := <-ch.rcvDone
	if msg.Cmd == CMD_OK || msg.Cmd == CMD_RCV_WAKEUP{
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

func (ch *redisChannel) onMessage(ev gnet.Event) {
	if msg, ok := ev.Message().(*Proto); !ok {
		logger.Errorln("msg type assert error")
		return
	} else {
		switch msg.Cmd {
		case CMD_OK:
			ch.onChOk(msg)
		case CMD_BLOCK:
			ch.onChBlock(msg)
		case CMD_CLOSED:
			ch.onChClosed(msg)

		case CMD_SND_WAKEUP:
			ch.onChSndWakeUp(msg)
		case CMD_RCV_WAKEUP:
			ch.onChRcvWakeUp(msg)

		case CMD_ERRNOTFOUND:
			logger.Errorln("channel not found, name:", msg.Name)
		case CMD_ERRPROTO:
			logger.Errorln("error protocol")
		case CMD_ERRCMD:
			logger.Errorln("error cmd:", msg.Cmd)

		default:
			logger.Errorln("unknown cmd:", msg.Cmd)
		}
	}
}

func (ch *redisChannel) onChOk(msg *Proto) {
	ch.l.Lock()
	defer ch.l.Unlock()

	switch ch.status {
	case NET_CREATE:
		ch.connected <- struct{}{}
	case NET_IN:
		ch.sndDone <- struct{}{}
	case NET_OUT:
		ch.rcvDone <- msg

	default:
		logger.Errorln("in onOk mode, error status:", ch.status)
	}
}

func (ch *redisChannel) onChBlock(msg *Proto) {
	ch.l.Lock()
	defer ch.l.Unlock()

	switch ch.status {
	case NET_IN:
		ch.status = BLOCK_IN
	case NET_OUT:
		ch.status = BLOCK_OUT
	default:
		logger.Errorln("in onBlock mode, error status:", ch.status)
	}

}

func (ch *redisChannel) onChClosed(msg *Proto) {
	ch.l.Lock()
	defer func() {
		ch.status = CLOSED
		ch.l.Unlock()
	}()

	switch ch.status {
	case NET_CREATE:
		ch.connected <- struct{}{}
	case NET_IN, BLOCK_IN:
		ch.sndDone <- struct{}{}
	case NET_OUT, BLOCK_OUT:
		ch.rcvDone <- msg
	default:
		logger.Errorln("in onClose mode, error status:", ch.status)
	}
}

func (ch *redisChannel) onChSndWakeUp(msg *Proto) {
	ch.l.Lock()
	defer ch.l.Unlock()

	switch ch.status {
	case BLOCK_IN:
		ch.sndDone <- struct{}{}
	default:
		logger.Errorln("in onSndWakeUp mode, error status:", ch.status)
	}
}

func (ch *redisChannel) onChRcvWakeUp(msg *Proto) {
	ch.l.Lock()
	defer ch.l.Unlock()

	switch ch.status {
	case BLOCK_OUT:
		ch.rcvDone <- msg
	default:
		logger.Errorln("in onRcvWakeUp mode, error status:", ch.status)
	}
}

func (ch *redisChannel) onConnected(s gnet.NetSession) {
	ch.netSession = s
	ch.status = NET_CREATE

	ch.netSession.Send(&Proto{
		Cmd:  CMD_NEWORCREAT,
		Name: ch.name,
		Data: []byte(strconv.Itoa(ch.cap)),
	})
}

func (ch *redisChannel) onClose(s gnet.NetSession) {

}
