package main

import (
	"github.com/MaxnSter/gnet/logger"
	. "github.com/MaxnSter/redis_channel"
	"reflect"
	"strconv"
	"strings"
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
	d := &data{
		Name: "ch",
		Nums: []int{1, 2, 3, 4, 5},
	}
	ch.In(d)

	logger.Infof("send:%s\n", d)
}
