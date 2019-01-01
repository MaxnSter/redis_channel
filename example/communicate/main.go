package main

import (
	"flag"
	"fmt"
	"github.com/MaxnSter/remote_channel"
	"math/rand"
	"sync"
	"time"
)

func main() {
	addr := flag.String("addr", ":8000", "remote channel server addr")
	ch := flag.String("ch", "ch", "remote channel name")
	flag.Parse()
	rand.Seed(time.Now().UnixNano())

	// init server
	go initServer(*addr)

	// init sender
	wg := sync.WaitGroup{}
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			initSender(*ch, 0, *addr)
			wg.Done()
		}()
	}

	// init receiver
	wgRcv := sync.WaitGroup{}
	wgRcv.Add(1)
	go func() {
		initReceiver(*ch, 0, *addr)
		wgRcv.Done()
	}()

	// send job done, close chanel
	wg.Wait()
	channel := remote_channel.NewRemoteChannel(*ch, remote_channel.GetChTypeInt64(), 0).Make(*addr)
	channel.Close()
	wgRcv.Wait()

	// receiver exit
	fmt.Println("exit")
}

func initServer(addr string) {
	remote_channel.NewChannelServer().ListenAndServe(addr)
}

func initSender(chName string, cap int, addr string) {
	dataType := remote_channel.GetChTypeInt64()
	ch := remote_channel.NewRemoteChannel(chName, dataType, cap).Make(addr)

	for i := 0; i < 3; i++ {
		ch.In(rand.Int63())
	}
}

func initReceiver(chName string, cap int, addr string) {
	dataType := remote_channel.GetChTypeInt64()
	ch := remote_channel.NewRemoteChannel(chName, dataType, cap).Make(addr)
	var counter int

	for {
		data, ok := ch.Out()
		if !ok {
			break
		}

		counter++
		fmt.Printf("recv:%d msg, val:%+v", counter, *data.(*int64))
	}
}
