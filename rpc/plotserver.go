package rpc

import (
	"fmt"
	"log"
	"net"

	"github.com/spacemeshos/post/config"
	"github.com/spacemeshos/post/initialization"
	"github.com/spacemeshos/post/internal/postrs"
	pb "github.com/spacemeshos/post/rpc/proto"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type PlotServiceServer struct{}

type PlotOption struct {
	IDHex              string
	CommitmentAtxIdHex string
	NumUnits           uint32
	Index              int
}

type Provider struct {
	postrs.Provider
	used bool
}

type Providers struct {
	Providers []Provider
}

func getProviders() []Provider {
	var providers, _ = postrs.OpenCLProviders()
	var gpu_providers = make([]Provider, len(providers)-1)
	for _, provider := range providers {
		if provider.DeviceType == 2 {
			var gpu_provider Provider
			gpu_provider.used = false
			gpu_providers = append(gpu_providers, gpu_provider)
		}
	}
	return gpu_providers
}

var providers = &Providers{
	Providers: getProviders(),
}

func (p *Providers) freeProviderID() *uint32 {
	for _, provider := range p.Providers {
		if !provider.used {
			p.switchUsed(provider.ID)
			return &provider.ID
		}
	}
	return nil
}

func (p *Providers) switchUsed(id uint32) {
	for _, provider := range p.Providers {
		if provider.ID == id {
			provider.used = !provider.used
			break
		}
	}
}

func (rps *PlotServiceServer) Plot(stream pb.PlotService_PlotServer) error {
	var logLevel zapcore.Level
	var cfg = config.MainnetConfig()
	var opts = config.MainnetInitOpts()

	request, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("rpc recv fail: %w", err)
	}

	opts.ProviderID = providers.freeProviderID()
	if opts.ProviderID == nil {
		fmt.Printf("no enough gpu to use.")
		return nil
	}

	zapCfg := zap.Config{
		Level:    zap.NewAtomicLevelAt(logLevel),
		Encoding: "console",
		EncoderConfig: zapcore.EncoderConfig{
			TimeKey:        "T",
			LevelKey:       "L",
			NameKey:        "N",
			MessageKey:     "M",
			LineEnding:     zapcore.DefaultLineEnding,
			EncodeLevel:    zapcore.CapitalLevelEncoder,
			EncodeTime:     zapcore.ISO8601TimeEncoder,
			EncodeDuration: zapcore.StringDurationEncoder,
		},
		OutputPaths:      []string{"stdout"},
		ErrorOutputPaths: []string{"stderr"},
	}
	logger, err := zapCfg.Build()
	if err != nil {
		log.Fatalln("failed to initialize zap logger:", err)
	}

	init, err := initialization.NewSingleInitializer(
		initialization.WithConfig(cfg),
		initialization.WithInitOpts(opts),
		initialization.WithNodeId(request.Id),
		initialization.WithCommitmentAtxId(request.CommitmentAtxId),
		initialization.WithLogger(logger),
		initialization.WithIndex(int(request.Index)),
	)
	if err != nil {
		log.Panic(err.Error())
	}

	if err := init.SingleInitialize(stream); err != nil {
		log.Panic(err)
	}

	defer providers.switchUsed(*opts.ProviderID)
	return nil
}

func PlotServer() {
	listener, err := net.Listen("tcp", ":1234")
	if err != nil {
		fmt.Println(err)
		return
	}

	ps := grpc.NewServer()
	reflection.Register(ps)
	pb.RegisterPlotServiceServer(ps, &PlotServiceServer{})
	fmt.Println("Server is listening on port 1234...")
	if err := ps.Serve(listener); err != nil {
		fmt.Printf("failed to serve: %v", err)
	}
}
