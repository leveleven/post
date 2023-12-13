package rpc

import (
	"encoding/hex"
	"fmt"
	"log"

	"github.com/spacemeshos/post/config"
	"github.com/spacemeshos/post/initialization"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type RemotePlotServer struct{}

type PlotOption struct {
	IDHex              string
	CommitmentAtxIdHex string
	NumUnits           uint32
}

func (rps *RemotePlotServer) plot(args *PlotOption, reply *initialization.Initializer) error {
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
	// opts.FromFileIdx = io.Index
	// opts.ToFileIdx = &io.Index
	// opts.NumUnits = io.NumUnits
	// opts.ProviderID = &io.Provider.ID
	// opts.DataDir = io.DataDir

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

	init, err := initialization.NewInitializer(
		initialization.WithConfig(cfg),
		initialization.WithInitOpts(opts),
		initialization.WithNodeId(id),
		initialization.WithCommitmentAtxId(commitmentAtxId),
		initialization.WithLogger(logger),
	)
	if err != nil {
		log.Panic(err.Error())
	}

	reply = init

	return nil
}
