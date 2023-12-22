package main

import (
	"flag"
	"log"

	"github.com/spacemeshos/post/rpc"
)

var (
	host string
	port string
)

func parseFlags() {
	flag.StringVar(&port, "port", "2345", "set host port")
	flag.StringVar(&host, "host", "127.0.0.1", "set host ip")

	flag.Parse()
}

func main() {
	var schedule_server rpc.ScheduleServer
	parseFlags()

	schedule_server.Host = host
	schedule_server.Port = port

	if err := schedule_server.RemoteScheduleServer(); err != nil {
		log.Fatalln("failed to start schedule server", err.Error())
		return
	}
}
