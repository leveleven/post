package rpc

import (
	"context"
	"fmt"
	"net"
	"sync"

	pb "github.com/spacemeshos/post/rpc/proto"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

const (
	offline = GPUStatusType(-1)
	idle    = GPUStatusType(0)
	running = GPUStatusType(1)
)

type ScheduleServer struct {
	Providers map[string]Provider
	Host      string
	Port      string
	logger    *zap.Logger

	mut sync.Mutex
	*pb.UnimplementedScheduleServiceServer
}

type GPUStatusType int32

type Provider struct {
	ID    uint32
	Model string
	UUID  string
	Host  string
	Port  string
	// InUse bool
	Status GPUStatusType
}

func (gst GPUStatusType) String() string {
	switch gst {
	case offline:
		return "offline"
	case idle:
		return "idle"
	case running:
		return "running"
	default:
		return "unknown"
	}
}

func NewScheduleServer(logger *zap.Logger) *ScheduleServer {
	return &ScheduleServer{
		Providers: make(map[string]Provider),
		logger:    logger,
	}
}

func (ss *ScheduleServer) AddProvider(ctx context.Context, request *pb.Provider) (*pb.UUID, error) {
	provider := Provider{
		ID:     request.ID,
		Model:  request.Model,
		UUID:   request.UUID,
		Host:   request.Host,
		Port:   request.Port,
		Status: 0,
	}
	ss.Providers[request.UUID] = provider
	ss.logger.Info("add new provider", zap.String("UUID", request.UUID))
	return &pb.UUID{UUID: provider.UUID}, nil
}

func (ss *ScheduleServer) SelectProvider(ctx context.Context, request *pb.UUID) (*pb.Provider, error) {
	provider := ss.Providers[request.GetUUID()]
	response := &pb.Provider{
		ID:     provider.ID,
		Model:  provider.Model,
		UUID:   provider.UUID,
		Host:   provider.Host,
		Port:   provider.Port,
		Status: int32(provider.Status),
	}
	return response, nil
}

func (ss *ScheduleServer) ChangeProviderStatus(ctx context.Context, request *pb.Pstatus) (*pb.Provider, error) {
	uuid := request.GetUUID()
	prev := ss.Providers[uuid]
	provider, err := ss.changeProviderStatus(uuid, request.Status)
	if err != nil {
		return &pb.Provider{}, err
	}

	response := &pb.Provider{
		ID:     provider.ID,
		Model:  provider.Model,
		UUID:   provider.UUID,
		Host:   provider.Host,
		Port:   provider.Port,
		Status: int32(provider.Status),
	}
	ss.logger.Info("switch provider in use",
		zap.String("UUID", provider.UUID),
		zap.String("current", prev.Status.String()),
		zap.String("new", GPUStatusType(provider.Status).String()),
	)
	return response, nil
}

func (ss *ScheduleServer) changeProviderStatus(uuid string, status int32) (Provider, error) {
	provider := ss.Providers[uuid]
	if provider.UUID == "" {
		return Provider{}, fmt.Errorf("can not find value of uuid: %s", uuid)
	}
	provider.Status = GPUStatusType(status)
	ss.Providers[uuid] = provider
	return provider, nil
}

func (ss *ScheduleServer) GetFreeProvider(ctx context.Context, empty *pb.Empty) (*pb.Provider, error) {
	ss.mut.Lock()
	defer ss.mut.Unlock()
	for _, provider := range ss.Providers {
		if provider.Status == 0 {
			response := &pb.Provider{
				ID:     provider.ID,
				Model:  provider.Model,
				UUID:   provider.UUID,
				Host:   provider.Host,
				Port:   provider.Port,
				Status: int32(provider.Status),
			}
			ss.changeProviderStatus(provider.UUID, 1)
			ss.logger.Info("find a idle provider",
				zap.Uint32("id", provider.ID),
				zap.String("model", provider.Model),
				zap.String("UUID", provider.UUID))
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
			ID:     value.ID,
			Model:  value.Model,
			UUID:   value.UUID,
			Host:   value.Host,
			Port:   value.Port,
			Status: int32(value.Status),
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
	// creds, err := credentials.NewServerTLSFromFile("./server.pem", "./server.key")
	// if err != nil {
	// 	return fmt.Errorf("Failed to load tls file: %v", err)
	// }
	// rss := grpc.NewServer(grpc.Creds(creds))
	rss := grpc.NewServer(grpc.Creds(nil))
	reflection.Register(rss)

	pb.RegisterScheduleServiceServer(rss, ss)
	ss.logger.Info("Schedule server is listening on " + ss.Host + ":" + ss.Port)
	if err := rss.Serve(listener); err != nil {
		return fmt.Errorf("failed to serve: %v", err)
	}
	return nil
}
