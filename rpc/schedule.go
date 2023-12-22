package rpc

import (
	"context"
	"fmt"
	"net"
	"sync"

	pb "github.com/spacemeshos/post/rpc/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/reflection"
)

type ScheduleServer struct {
	Providers map[string]Provider
	Host      string
	Port      string
	mut       sync.Mutex

	*pb.UnimplementedScheduleServiceServer
}

type Provider struct {
	// postrs.Provider
	ID    uint32
	Model string
	UUID  string
	Host  string
	Port  string
	InUse bool
}

func (ss *ScheduleServer) AddProvider(ctx context.Context, request *pb.Provider) (*pb.UUID, error) {
	// ss.mut.Lock()
	// defer ss.mut.Unlock()
	// request, err := stream.Recv()
	// if err != nil {
	// 	return fmt.Errorf("rpc recv fail: %w", err)
	// }
	provider := Provider{
		ID:    request.ID,
		Model: request.Model,
		UUID:  request.UUID,
		Host:  request.Host,
		Port:  request.Port,
		InUse: false,
	}
	ss.Providers[request.UUID] = provider
	// if err := stream.Send(&pb.UUID{UUID: provider.UUID}); err != nil {
	// 	return err
	// }
	return &pb.UUID{UUID: provider.UUID}, nil
}

func (ss *ScheduleServer) SelectProvider(ctx context.Context, request *pb.UUID) (*pb.Provider, error) {
	provider := ss.Providers[request.GetUUID()]
	response := &pb.Provider{
		ID:    provider.ID,
		Model: provider.Model,
		UUID:  provider.UUID,
		Host:  provider.Host,
		Port:  provider.Port,
		InUse: provider.InUse,
	}
	return response, nil
}

func (ss *ScheduleServer) SwitchProvider(ctx context.Context, request *pb.UUID) (*pb.Provider, error) {
	uuid := request.GetUUID()
	provider := ss.Providers[uuid]
	provider.InUse = !provider.InUse
	ss.Providers[uuid] = provider

	response := &pb.Provider{
		ID:    provider.ID,
		Model: provider.Model,
		UUID:  provider.UUID,
		Host:  provider.Host,
		Port:  provider.Port,
		InUse: provider.InUse,
	}
	return response, nil
}

func (ss *ScheduleServer) GetFreeProvider(ctx context.Context, empty *pb.Empty) (*pb.Provider, error) {
	for _, provider := range ss.Providers {
		if !provider.InUse {
			response := &pb.Provider{
				ID:    provider.ID,
				Model: provider.Model,
				UUID:  provider.UUID,
				Host:  provider.Host,
				Port:  provider.Port,
				InUse: provider.InUse,
			}
			return response, nil
		}
	}
	return nil, nil
}

func (ss *ScheduleServer) ShowProviders(ctx context.Context, empty *pb.Empty) (*pb.Providers, error) {
	var s_providers pb.Providers
	for key, value := range ss.Providers {
		s_providers.Providers[key] = &pb.Provider{
			ID:    value.ID,
			Model: value.Model,
			UUID:  value.UUID,
			Host:  value.Host,
			Port:  value.Port,
			InUse: value.InUse,
		}
	}
	return &s_providers, nil
}

func (ss *ScheduleServer) RemoteScheduleServer() error {
	listener, err := net.Listen("tcp", ss.Host+":"+ss.Port)
	if err != nil {
		return fmt.Errorf("failed to listen %s: %v", ss.Host+":"+ss.Port, err)
	}
	schedule := new(ScheduleServer)

	// tls
	creds, err := credentials.NewServerTLSFromFile("server.crt", "server.key")
	rss := grpc.NewServer(grpc.Creds(creds))
	reflection.Register(rss)
	pb.RegisterScheduleServiceServer(rss, schedule)
	fmt.Println("Server is listening on " + ss.Host + ":" + ss.Port)
	if err := rss.Serve(listener); err != nil {
		return fmt.Errorf("failed to serve: %v", err)
	}
	return nil
}
