package rpc

import (
	"fmt"
	"net"
	"sync"

	"github.com/spacemeshos/post/internal/postrs"
	pb "github.com/spacemeshos/post/rpc/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type ScheduleServer struct {
	*pb.UnimplementedScheduleServiceServer
	Providers map[string]Provider
	mut       sync.Mutex
}

type Provider struct {
	postrs.Provider
	UUID  string
	Host  string
	Port  int
	InUse bool
}

func (ss *ScheduleServer) AddProvider(stream pb.ScheduleService_AddProviderServer) error {
	request, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("rpc recv fail: %w", err)
	}
	provider := Provider{
		Provider: postrs.Provider{
			ID:         request.GetID(),
			Model:      request.GetModel(),
			DeviceType: postrs.DeviceClass(request.GetDeviceType()),
		},
		UUID:  request.GetUUID(),
		Host:  request.GetHost(),
		Port:  int(request.GetPort()),
		InUse: false,
	}

	ss.Providers[provider.UUID] = provider
	stream.Send(&pb.UUID{UUID: provider.UUID})
	return nil
}

func (ss *ScheduleServer) SelectProvider(stream pb.ScheduleService_SelectProviderServer) error {
	request, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("rpc recv fail: %w", err)
	}
	provider := ss.Providers[request.GetUUID()]
	response := &pb.Provider{
		ID:         provider.ID,
		Model:      provider.Model,
		DeviceType: int32(provider.DeviceType),
		UUID:       provider.UUID,
		Host:       provider.Host,
		Port:       int32(provider.Port),
		InUse:      provider.InUse,
	}
	stream.Send(response)
	return nil
}

func (ss *ScheduleServer) SwitchProvider(stream pb.ScheduleService_SwitchProviderServer) error {
	request, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("rpc recv fail: %w", err)
	}
	uuid := request.GetUUID()
	provider := ss.Providers[uuid]
	provider.InUse = !provider.InUse
	ss.Providers[uuid] = provider

	response := &pb.Provider{
		ID:         provider.ID,
		Model:      provider.Model,
		DeviceType: int32(provider.DeviceType),
		UUID:       provider.UUID,
		Host:       provider.Host,
		Port:       int32(provider.Port),
		InUse:      provider.InUse,
	}
	stream.Send(response)
	return nil
}

func (ss *ScheduleServer) GetFreeProvider(stream pb.ScheduleService_GetFreeProviderServer) error {
	for _, provider := range ss.Providers {
		if !provider.InUse {
			response := &pb.Provider{
				ID:         provider.ID,
				Model:      provider.Model,
				DeviceType: int32(provider.DeviceType),
				UUID:       provider.UUID,
				Host:       provider.Host,
				Port:       int32(provider.Port),
				InUse:      provider.InUse,
			}
			stream.Send(response)
		}
	}
	return nil
}

func RemoteScheduleServer() {
	listener, err := net.Listen("tcp", ":2345")
	if err != nil {
		fmt.Println(err)
		return
	}
	schedule := new(ScheduleServer)

	rss := grpc.NewServer()
	reflection.Register(rss)
	pb.RegisterScheduleServiceServer(rss, schedule)
	fmt.Println("Server is listening on port 1234...")
	if err := rss.Serve(listener); err != nil {
		fmt.Printf("failed to serve: %v", err)
	}
}
