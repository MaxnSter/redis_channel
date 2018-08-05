package main

import "github.com/MaxnSter/redis_channel/redis_channel_server"

func main() {
	redis_channel_server.NewChannelServer().ListenAndServe("0.0.0.0:2007")
}
