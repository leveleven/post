package rpc

import (
	"context"
	"fmt"
	"net"
	"sync"

	pb "github.com/spacemeshos/post/rpc/proto"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/reflection"
)

type ScheduleServer struct {
	Providers map[string]Provider
	Host      string
	Port      string
	logger    *zap.Logger

	mut sync.Mutex
	*pb.UnimplementedScheduleServiceServer
}

type Provider struct {
	ID    uint32
	Model string
	UUID  string
	Host  string
	Port  string
	InUse bool
}

func NewScheduleServer(logger *zap.Logger) *ScheduleServer {
	return &ScheduleServer{
		Providers: make(map[string]Provider),
		logger:    logger,
	}
}

func (ss *ScheduleServer) AddProvider(ctx context.Context, request *pb.Provider) (*pb.UUID, error) {
	provider := Provider{
		ID:    request.ID,
		Model: request.Model,
		UUID:  request.UUID,
		Host:  request.Host,
		Port:  request.Port,
		InUse: false,
	}
	ss.Providers[request.UUID] = provider
	ss.logger.Info("add new provider", zap.String("UUID", request.UUID))
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
	provider, err := ss.switchProvider(uuid)
	if err != nil {
		return &pb.Provider{}, err
	}

	response := &pb.Provider{
		ID:    provider.ID,
		Model: provider.Model,
		UUID:  provider.UUID,
		Host:  provider.Host,
		Port:  provider.Port,
		InUse: provider.InUse,
	}
	ss.logger.Info("switch provider in use",
		zap.String("UUID", provider.UUID),
		zap.Bool("current", !provider.InUse),
		zap.Bool("new", provider.InUse),
	)
	return response, nil
}

func (ss *ScheduleServer) switchProvider(uuid string) (Provider, error) {
	provider := ss.Providers[uuid]
	if provider.UUID == "" {
		return Provider{}, fmt.Errorf("can not find value of uuid: ", uuid)
	}
	provider.InUse = !provider.InUse
	ss.Providers[uuid] = provider
	return provider, nil
}

func (ss *ScheduleServer) GetFreeProvider(ctx context.Context, empty *pb.Empty) (*pb.Provider, error) {
	ss.mut.Lock()
	defer ss.mut.Unlock()
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
			ss.switchProvider(provider.UUID)
			ss.logger.Info("find a idle provider", zap.String("UUID", provider.UUID))
			return response, nil
		}
	}
	return &pb.Provider{}, nil
}

func (ss *ScheduleServer) ShowProviders(ctx context.Context, empty *pb.Empty) (*pb.Providers, error) {
	var s_providers []*pb.Provider
	s_providers = make([]*pb.Provider, 0)
	for _, value := range ss.Providers {
		s_providers = append(s_providers, &pb.Provider{
			ID:    value.ID,
			Model: value.Model,
			UUID:  value.UUID,
			Host:  value.Host,
			Port:  value.Port,
			InUse: value.InUse,
		})
	}
	return &pb.Providers{Provider: s_providers}, nil
}

func (ss *ScheduleServer) RemoteScheduleServer() error {
	listener, err := net.Listen("tcp", ss.Host+":"+ss.Port)
	if err != nil {
		return fmt.Errorf("failed to listen %s: %v", ss.Host+":"+ss.Port, err)
	}

	// tls
	creds, err := credentials.NewServerTLSFromFile("server.crt", "server.key")
	rss := grpc.NewServer(grpc.Creds(creds))
	reflection.Register(rss)
	pb.RegisterScheduleServiceServer(rss, ss)
	fmt.Println("Schedule server is listening on " + ss.Host + ":" + ss.Port)
	if err := rss.Serve(listener); err != nil {
		return fmt.Errorf("failed to serve: %v", err)
	}
	return nil
}
