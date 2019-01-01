package remote_channel

import (
	"github.com/MaxnSter/gnet/message_pack/message_meta"
	"reflect"
)

type Command int

const (
	// 请求码
	CMD_NEWORCREAT = iota
	CMD_IN
	CMD_OUT
	CMD_CLOSE

	// 响应码
	CMD_CREATE_OK
	CMD_SND_OK
	CMD_RCV_OK
	CMD_SND_BLOCK
	CMD_RCV_BLOCK
	CMD_CLOSED
	CMD_SND_WAKEUP
	CMD_RCV_WAKEUP
	CMD_ERRNOTFOUND
	CMD_ERRCMD
	CMD_ERRPROTO
)

func init() {
	message_meta.RegisterMsgMeta(&message_meta.MessageMeta{
		ID:   0,
		Type: reflect.TypeOf((*Proto)(nil)),
	})
}

type Proto struct {
	Id   uint32
	Cmd  Command
	Name string
	Data []byte
}

func (p *Proto) GetId() uint32 {
	return 0
}

