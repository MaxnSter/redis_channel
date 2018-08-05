package main

import (
	"strconv"
	"strings"

	"reflect"

	"github.com/MaxnSter/gnet/logger"
	. "github.com/MaxnSter/redis_channel"
)

type data struct {
	Name string
	Nums []int
}

func (d *data) String() string {
	sb := &strings.Builder{}

	sb.WriteString(d.Name)
	for _, num := range d.Nums {
		sb.WriteByte(',')
		sb.WriteString(strconv.Itoa(num))
	}

	return sb.String()
}

func main() {
	ch := NewRedisChannel(0,
		"channel",
		reflect.TypeOf((*data)(nil))).Make("127.0.0.1:2007")
	if v, ok := ch.Out(); !ok {
		logger.Errorln("error")
	} else {
		logger.Infof("val:%s\n", v.(*data))
	}
}
