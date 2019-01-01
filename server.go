package server

import (
	"github.com/MaxnSter/GolangDataStructure/leetcode/queue"
	"github.com/MaxnSter/gnet"

	"bufio"
	"fmt"
	"os"
	"strconv"

	_ "github.com/MaxnSter/gnet/codec/codec_msgpack"
	"github.com/MaxnSter/gnet/iface"
	"github.com/MaxnSter/gnet/logger"
	_ "github.com/MaxnSter/gnet/message_pack/pack/pack_type_length_value"
	_ "github.com/MaxnSter/gnet/net/tcp"
	"github.com/MaxnSter/gnet/worker_pool"
	_ "github.com/MaxnSter/gnet/worker_pool/worker_session_race_other"
	cmd "github.com/MaxnSter/remote_channel"
)

type sndWaitEntry struct {
	id   int64
	data []byte
}

type remoteCh struct {
	cap int
	buf *queue.Queue

	sndWaitList *queue.Queue
	rcvWaitList *queue.Queue
}

type channelServer struct {
	buffers   map[string]*remoteCh
	netServer gnet.NetServer
	eventPool worker_pool.Pool
}

func NewChannelServer() *channelServer {
	s := &channelServer{
		buffers: make(map[string]*remoteCh),
	}

	m := gnet.NewModule(gnet.WithCoder("msgpack"), gnet.WithPacker("tlv"),
		gnet.WithPool("poolRaceOther"))
	o := gnet.NewOperator(s.onMessage)

	s.netServer = gnet.NewNetServer("tcp", "channel_server", m, o)
	s.eventPool = m.Pool()

	return s
}

func (s *channelServer) ListenAndServe(addr string) {
	go s.console()
	s.netServer.ListenAndServe(addr)
}

func (s *channelServer) onMessage(ev gnet.Event) {
	msg, ok := ev.Message().(*cmd.Proto)
	if !ok {
		ev.Session().Send(&cmd.Proto{
			Cmd: cmd.CMD_ERRPROTO,
		})
		return
	}

	logger.Debugf("cmd = %d\n", msg.Cmd)
	switch msg.Cmd {
	case cmd.CMD_NEWORCREAT:
		s.newOrCreate(msg, ev.Session())
	case cmd.CMD_CLOSE:
		s.close(msg, ev.Session())
	case cmd.CMD_IN:
		s.in(msg, ev.Session())
	case cmd.CMD_OUT:
		s.out(msg, ev.Session())
	default:
		// 未知命令
		ev.Session().Send(&cmd.Proto{
			Cmd: cmd.CMD_ERRCMD,
		})
	}
}

func (s *channelServer) in(msg *cmd.Proto, session gnet.NetSession) {
	var ch *remoteCh
	var hit bool

	if ch, hit = s.buffers[msg.Name]; !hit {
		session.Send(&cmd.Proto{
			Cmd:  cmd.CMD_ERRNOTFOUND,
			Name: msg.Name,
		})
		return
	}

	logger.Debugln("channel in mode")
	if ch.rcvWaitList.Size() > 0 {
		logger.Debugln("rcvWaitList not empty")
		for ch.rcvWaitList.Size() > 0 {
			sID := ch.rcvWaitList.Peek().(int64)
			ch.rcvWaitList.PopFront()

			if sWait, ok := s.netServer.GetSession(sID); !ok {
				logger.Debugln("rcvWaiter not online, sid:", sID)
				continue
			} else {
				logger.Debugln("rcvWaiter online, sid:", sID)
				// 唤醒阻塞中的接受者, directly send msg.data
				msg.Cmd = cmd.CMD_RCV_WAKEUP
				sWait.Send(msg)

				session.Send(&cmd.Proto{
					Cmd:  cmd.CMD_SND_OK,
					Name: msg.Name,
				})
				return
			}
		}
	}

	if ch.buf.Size() >= ch.cap {
		ch.sndWaitList.PushBack(&sndWaitEntry{
			id:   session.ID(),
			data: msg.Data,
		})

		session.Send(&cmd.Proto{
			Cmd:  cmd.CMD_SND_BLOCK,
			Name: msg.Name,
		})
		return
	}

	ch.buf.PushBack(msg.Data)
	session.Send(&cmd.Proto{
		Cmd:  cmd.CMD_SND_OK,
		Name: msg.Name,
	})
}

func (s *channelServer) outSync(ch *remoteCh, msg *cmd.Proto, session gnet.NetSession) {
	if ch.sndWaitList.Size() > 0 {
		sndWaitBuf := ch.sndWaitList.Peek().(*sndWaitEntry)
		ch.sndWaitList.PopFront()

		msg.Data = sndWaitBuf.data
		msg.Cmd = cmd.CMD_RCV_OK
		session.Send(msg)

		// 唤醒发送者
		if sessionWait, ok := s.netServer.GetSession(sndWaitBuf.id); ok {
			sessionWait.Send(&cmd.Proto{
				Cmd:  cmd.CMD_SND_WAKEUP,
				Name: msg.Name,
			})
		}
	} else {
		ch.rcvWaitList.PushBack(session.ID())
		msg.Cmd = cmd.CMD_RCV_BLOCK
		session.Send(msg)
	}
}

func (s *channelServer) out(msg *cmd.Proto, session gnet.NetSession) {
	var ch *remoteCh
	var hit bool

	if ch, hit = s.buffers[msg.Name]; !hit {
		session.Send(&cmd.Proto{
			Cmd:  cmd.CMD_ERRNOTFOUND,
			Name: msg.Name,
		})
		return
	}

	if ch.cap == 0 {
		// 同步模式
		s.outSync(ch, msg, session)
		return
	}

	// 缓存中没有数据,block
	if ch.buf.Size() <= 0 {
		ch.rcvWaitList.PushBack(session.ID())
		session.Send(&cmd.Proto{
			Cmd:  cmd.CMD_RCV_BLOCK,
			Name: msg.Name,
		})
		return
	}

	// 取出队列中的数据
	data := ch.buf.Peek().([]byte)
	ch.buf.PopFront()

	msg.Data = data
	msg.Cmd = cmd.CMD_RCV_OK
	session.Send(msg)

	// 唤醒待发送队列中的sender
	if ch.sndWaitList.Size() > 0 {
		sndWaitBuf := ch.sndWaitList.Peek().(*sndWaitEntry)

		ch.buf.PushBack(sndWaitBuf.data)
		if sessionWait, ok := s.netServer.GetSession(sndWaitBuf.id); ok {
			sessionWait.Send(&cmd.Proto{
				Cmd:  cmd.CMD_SND_WAKEUP,
				Name: msg.Name,
			})
		}
	}

}

func (s *channelServer) newOrCreate(msg *cmd.Proto, session gnet.NetSession) {
	var chCap int
	var err error
	if chCap, err = strconv.Atoi(string(msg.Data)); err != nil {
		logger.Errorln("error at atoi:%s", err)
		msg.Cmd = cmd.CMD_ERRPROTO
		session.Send(msg)
		return
	}

	if _, ok := s.buffers[msg.Name]; !ok {
		s.buffers[msg.Name] = &remoteCh{
			cap:         chCap,
			buf:         queue.NewQueue(),
			sndWaitList: queue.NewQueue(),
			rcvWaitList: queue.NewQueue(),
		}
	}

	msg.Cmd = cmd.CMD_CREATE_OK
	session.Send(msg)
}

func (s *channelServer) close(msg *cmd.Proto, session gnet.NetSession) {
	var ch *remoteCh
	var hit bool

	logger.Debugln("closing every one!!!")

	if ch, hit = s.buffers[msg.Name]; !hit {
		msg.Cmd = cmd.CMD_ERRNOTFOUND
		session.Send(msg)
		return
	}

	delete(s.buffers, msg.Name)
	for ch.sndWaitList.Size() > 0 {
		logger.Debugln("closing sender")

		sid := ch.sndWaitList.Peek().(*sndWaitEntry).id
		ch.sndWaitList.PopFront()

		if sWait, ok := s.netServer.GetSession(sid); ok {
			sWait.Send(&cmd.Proto{
				Cmd:  cmd.CMD_CLOSED,
				Name: msg.Name,
			})
		}
	}

	for ch.rcvWaitList.Size() > 0 {
		logger.Debugln("closing waiter")

		rid := ch.rcvWaitList.Peek().(int64)
		ch.rcvWaitList.PopFront()

		if rWait, ok := s.netServer.GetSession(rid); ok {
			rWait.Send(&cmd.Proto{
				Cmd:  cmd.CMD_CLOSED,
				Name: msg.Name,
			})
		}
	}
}

func (s *channelServer) dump() {
	fmt.Printf("channel size:%d\n", len(s.buffers))
	for name, ch := range s.buffers {
		fmt.Printf("name=%s,len=%d,cap=%d,sndWaiters:%d,rcvWaiters:%d\n",
			name, ch.buf.Size(), ch.cap, ch.sndWaitList.Size(), ch.rcvWaitList.Size())
	}
}

func (s *channelServer) console() {
	scan := bufio.NewScanner(os.Stdin)
	for scan.Scan() {
		switch scan.Text() {
		case "dump":
			s.eventPool.Put(nil, func(_ iface.Context) {
				s.dump()
			})
		default:
			fmt.Println("unkown console command:", scan.Text())
		}
	}
}
