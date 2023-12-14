package rpc

import (
	"context"
	"encoding/hex"
	"fmt"
	"log"
	"net"
	"net/rpc"

	"github.com/spacemeshos/post/config"
	"github.com/spacemeshos/post/initialization"
	"github.com/spacemeshos/post/internal/postrs"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type RemotePlotServer struct {
	Providers []Provider
}

type PlotOption struct {
	IDHex              string
	CommitmentAtxIdHex string
	NumUnits           uint32
	Index              int
	ctx                context.Context
}

type Provider struct {
	postrs.Provider
	used bool
}

func (rps *RemotePlotServer) freeProviderID() *uint32 {
	for _, provider := range rps.Providers {
		if !provider.used {
			rps.switchUsed(provider.ID)
			return &provider.ID
		}
	}
	return nil
}

func (rps *RemotePlotServer) switchUsed(id uint32) {
	for _, provider := range rps.Providers {
		if provider.ID == id {
			provider.used = !provider.used
			break
		}
	}
}

func (rps *RemotePlotServer) plot(args *PlotOption, reply *initialization.InitializerSingle) error {
	var logLevel zapcore.Level
	var cfg = config.MainnetConfig()
	var opts = config.MainnetInitOpts()

	commitmentAtxId, err := hex.DecodeString(args.CommitmentAtxIdHex)
	if err != nil {
		return fmt.Errorf("invalid commitmentAtxId: %w", err)
	}
	id, err := hex.DecodeString(args.IDHex)
	if err != nil {
		return fmt.Errorf("invalid id: %w", err)
	}

	opts.ProviderID = rps.freeProviderID()
	if opts.ProviderID == nil {
		return fmt.Errorf("no enough gpu to use.")
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
		initialization.WithNodeId(id),
		initialization.WithCommitmentAtxId(commitmentAtxId),
		initialization.WithLogger(logger),
		initialization.WithIndex(args.Index),
	)
	if err != nil {
		log.Panic(err.Error())
	}

	if err := init.SingleInitialize(args.ctx); err != nil {
		log.Panic(err)
	}

	defer rps.switchUsed(*opts.ProviderID)
	return nil
}

func PlotServer() {
	var providers, _ = postrs.OpenCLProviders()
	var gpu_providers = make([]Provider, len(providers)-1)
	for _, provider := range providers {
		if provider.DeviceType == 2 {
			var gpu_provider Provider
			gpu_provider.used = false
			gpu_providers = append(gpu_providers, gpu_provider)
		}
	}

	remote := &RemotePlotServer{
		gpu_providers,
	}

	rpc.Register(remote)
	listener, err := net.Listen("tcp", ":1234")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}
		go rpc.ServeConn(conn)
	}
}
