package rpc

import (
	"context"
	"fmt"
	"log"
	"net"

	"github.com/google/uuid"
	"github.com/spacemeshos/post/config"
	"github.com/spacemeshos/post/initialization"
	"github.com/spacemeshos/post/internal/postrs"
	pb "github.com/spacemeshos/post/rpc/proto"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
)

type PlotServer struct {
	Host     string
	Port     string
	Schedule string

	Logger *zap.Logger

	*pb.UnimplementedPlotServiceServer
}

type GPUProvider struct {
	ID    uint32
	Model string
	UUID  string
}

func GetProviders() ([]GPUProvider, error) {
	var providers, err = postrs.OpenCLProviders()
	if err != nil {
		return nil, err
	}
	var gpu_providers = make([]GPUProvider, 0)
	for _, provider := range providers {
		if provider.DeviceType.String() == "GPU" {
			gpu_provider := GPUProvider{
				ID:    provider.ID,
				Model: provider.Model,
				UUID:  uuid.New().String(),
			}
			gpu_providers = append(gpu_providers, gpu_provider)
		}
	}
	return gpu_providers, nil
}

func (ps *PlotServer) Plot(request *pb.Task, stream pb.PlotService_PlotServer) error {
	provider := &request.Provider.ID
	if provider == nil {
		return fmt.Errorf("no enough gpu to use")
	}

	ps.Logger.Info("Get task",
		zap.Int64("task index", request.Index),
		zap.Binary("node id", request.Id),
		zap.Uint32("numUnits", request.NumUnits),
		zap.Uint32("using provider id", *provider))

	cfg := config.MainnetConfig()
	cfg.LabelsPerUnit = request.LabelsPerUnit
	opt := config.MainnetInitOpts()
	opt.MaxFileSize = request.MaxFileSize

	init, err := initialization.NewSingleInitializer(
		initialization.WithConfig(cfg),
		initialization.WithInitOpts(opt),
		initialization.WithNodeId(request.Id),
		initialization.WithCommitmentAtxId(request.CommitmentAtxId),
		initialization.WithLogger(ps.Logger),
		initialization.WithIndex(request.Index),
		initialization.WithNumLabelsWritten(request.NumLabelsWritten),
	)
	if err != nil {
		log.Panic(err.Error())
	}

	if err := init.SingleInitialize(provider, stream); err != nil {
		log.Panic(err)
	}

	return nil
}

func (ps *PlotServer) submitPlot() error {
	// tls
	// creds, err := credentials.NewClientTLSFromFile("server.pem", "xjxh")
	// if err != nil {
	// 	return fmt.Errorf("Failed to load tls file: %v", err)
	// }
	// connect, err := grpc.Dial(ps.Schedule, grpc.WithTransportCredentials(creds))
	connect, err := grpc.Dial(ps.Schedule, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed connecting to server: %w", err)
	}
	defer connect.Close()
	client := pb.NewScheduleServiceClient(connect)

	gpu_provider, err := GetProviders()
	if err != nil {
		return err
	}

	if len(gpu_provider) == 0 {
		return fmt.Errorf("no found any gpu device")
	}
	for _, provider := range gpu_provider {
		uuid, err := client.AddProvider(
			context.Background(),
			&pb.Provider{
				ID:    provider.ID,
				Model: provider.Model,
				UUID:  provider.UUID,
				Host:  ps.Host,
				Port:  ps.Port,
			},
		)
		if err != nil {
			return fmt.Errorf("failed to call method: %w", err)
		}
		ps.Logger.Info("submit plot node",
			zap.String("UUID", uuid.UUID),
			zap.String("model", provider.Model),
		)
	}
	// 获取本机ip 端口
	return nil
}

func (ps *PlotServer) RemotePlotServer() error {
	if err := ps.submitPlot(); err != nil {
		return fmt.Errorf("failed to submit plot node: %w", err)
	}

	listener, err := net.Listen("tcp", ps.Host+":"+ps.Port)
	if err != nil {
		return fmt.Errorf("failed to listen"+ps.Host+":"+ps.Port+": %w", err)
	}

	rps := grpc.NewServer(grpc.Creds(nil))
	reflection.Register(rps)
	pb.RegisterPlotServiceServer(rps, ps)
	ps.Logger.Info("Plot server is listening on " + ps.Host + ":" + ps.Port)
	if err := rps.Serve(listener); err != nil {
		return fmt.Errorf("failed to serve: %w", err)
	}
	return nil
}
