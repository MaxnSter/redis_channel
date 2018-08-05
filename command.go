package redis_channel

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
	CMD_OK
	CMD_BLOCK
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

type ChType int

const (
	INT16 = iota
	INT
	INT32
	INT64

	FLOAT32
	FLOAT64

	BYTE
	STRING

	SLICEBYTE
)

func GetChType(t ChType) reflect.Type {
	switch t {
	case INT16:
		return reflect.TypeOf((*int16)(nil))
	case INT:
		return reflect.TypeOf((*int)(nil))
	case INT32:
		return reflect.TypeOf((*int32)(nil))
	case INT64:
		return reflect.TypeOf((*int64)(nil))
	case FLOAT32:
		return reflect.TypeOf((*float32)(nil))
	case FLOAT64:
		return reflect.TypeOf((*float64)(nil))
	case BYTE:
		return reflect.TypeOf((*byte)(nil))
	case STRING:
		return reflect.TypeOf((*string)(nil))
	case SLICEBYTE:
		return reflect.TypeOf((*[]byte)(nil))
	default:
		return nil
	}
}

type ChStatus int

const (
	NORMAL = iota
	CLOSED

	NET_IN
	NET_OUT
	NET_CREATE

	BLOCK_IN
	BLOCK_OUT
)
