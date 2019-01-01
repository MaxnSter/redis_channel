package remote_channel

import "reflect"

type ChType int

const (
	INT16 = iota
	INT
	INT32
	INT64
	UINT16
	UINT
	UINT32
	UINT64

	FLOAT32
	FLOAT64

	BYTE
	STRING

	SLICEBYTE
)

func GetChTypeInt64() reflect.Type {
	return GetChType(INT64)
}

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
