package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/davecgh/go-spew/spew"
	"github.com/google/uuid"
	"github.com/spacemeshos/post/internal/postrs"
	"github.com/spacemeshos/post/rpc"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	host     string
	port     string
	schedule string

	printProviders bool

	logLevel zapcore.Level
)

func parseFlags() {
	flag.TextVar(&logLevel, "logLevel", zapcore.InfoLevel, "log level (debug, info, warn, error, dpanic, panic, fatal)")
	flag.StringVar(&port, "port", "1234", "set host port")
	flag.StringVar(&host, "host", "127.0.0.1", "set host ip")
	flag.StringVar(&schedule, "schedule", "127.0.0.1:2345", "set schedule node ip:port")

	flag.BoolVar(&printProviders, "printProviders", false, "print the list of compute providers")

	flag.Parse()
}

func main() {
	var plot_server rpc.PlotServer
	parseFlags()

	if printProviders {
		var providers, err = postrs.OpenCLProviders()
		if err != nil {
			fmt.Println(err)
			return
		}
		var gpu_providers = make([]rpc.GPUProvider, 0)
		for _, provider := range providers {
			if provider.DeviceType.String() == "GPU" {
				gpu_provider := rpc.GPUProvider{
					ID:    provider.ID,
					Model: provider.Model,
					UUID:  uuid.New().String(),
				}
				gpu_providers = append(gpu_providers, gpu_provider)
			}
		}
		spew.Dump(gpu_providers)
		return
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

	plot_server.Host = host
	plot_server.Port = port
	plot_server.Schedule = schedule
	plot_server.Logger = logger

	if err := plot_server.RemotePlotServer(); err != nil {
		log.Fatalln("failed to start plot server:", err)
		return
	}
}
