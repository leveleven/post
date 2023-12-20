package rpc

import (
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"sync"

	"github.com/spacemeshos/post/internal/postrs"
)

type ScheduleServer struct {
	Providers map[string]Provider_GPU
	mut       sync.Mutex
}

type Provider_GPU struct {
	postrs.Provider
	UUID  string
	Host  string
	Port  int
	inUse bool
}

func (ss *ScheduleServer) addProvider(pgpu Provider_GPU) string {
	uuid := pgpu.UUID
	ss.Providers[uuid] = pgpu
	return uuid
}

func (ss *ScheduleServer) selectProvider(uuid string) Provider_GPU {
	return ss.Providers[uuid]
}

func (ss *ScheduleServer) switchProvider(uuid string) Provider_GPU {
	updateProvider := ss.Providers[uuid]
	updateProvider.inUse = !updateProvider.inUse
	ss.Providers[uuid] = updateProvider
	return ss.Providers[uuid]
}

func (ss *ScheduleServer) getFreeProvider() *Provider_GPU {
	for _, valus := range ss.Providers {
		if !valus.inUse {
			return &valus
		}
	}
	return nil
}

func RemoteScheduleRPC() {
	schedule := new(ScheduleServer)
	rpc.Register(schedule)

	rpc.HandleHTTP()
	listener, err := net.Listen("tcp", ":2345")
	if err != nil {
		fmt.Errorf(err.Error())
	}

	go http.Serve(listener, nil)
}
