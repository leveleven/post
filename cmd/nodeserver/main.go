package main

import (
	"flag"
	"log"
	"path/filepath"

	"github.com/spacemeshos/post/config"
	"github.com/spacemeshos/post/rpc"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var DefaultDataDir = filepath.Join("/data", "post")

var (
	opts = config.MainnetInitOpts()

	host     string
	port     string
	schedule string

	idHex              string
	id                 []byte
	commitmentAtxIdHex string
	numUnits           rpc.NumUnitsFlag
	parallel           int
	LabelsPerUnit      uint64

	logLevel zapcore.Level
)

func parseFlags() {
	flag.TextVar(&logLevel, "logLevel", zapcore.InfoLevel, "log level (debug, info, warn, error, dpanic, panic, fatal)")
	flag.StringVar(&port, "port", "1234", "set host port")
	flag.StringVar(&host, "host", "127.0.0.1", "set host ip")
	flag.StringVar(&schedule, "schedule", "127.0.0.1:2345", "set schedule node ip:port")
	flag.StringVar(&opts.DataDir, "datadir", DefaultDataDir, "filesystem datadir path")
	flag.StringVar(&commitmentAtxIdHex, "commitmentAtxId", "", "commitment atx id, in hex (required)")
	flag.Var(&numUnits, "numUnits", "number of units")
	flag.IntVar(&parallel, "parallel", 40, "parallel plot number, depend on your disk bandwidth (default 40)")

	flag.Uint64Var(&opts.MaxFileSize, "maxFileSize", config.MainnetInitOpts().MaxFileSize, "max file size")
	flag.Uint64Var(&LabelsPerUnit, "labelsPerUnit", config.MainnetConfig().LabelsPerUnit, "the number of labels per unit")

	flag.Parse()
}

func main() {
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

	server := rpc.NodeServer{
		Host:            host,
		Port:            port,
		Schedule:        schedule,
		CommitmentAtxId: commitmentAtxIdHex,
		NumUnits:        numUnits,
		LabelsPerUnit:   LabelsPerUnit,
		Opts:            &opts,
		Logger:          logger,
	}

	// 启动服务
	server.RemoteNodeServer()
}
