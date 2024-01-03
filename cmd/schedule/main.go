package main

import (
	"flag"
	"log"

	"github.com/spacemeshos/post/rpc"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	host string
	port string

	logLevel zapcore.Level
)

func parseFlags() {
	flag.TextVar(&logLevel, "logLevel", zapcore.InfoLevel, "log level (debug, info, warn, error, dpanic, panic, fatal)")
	flag.StringVar(&port, "port", "2345", "set host port")
	flag.StringVar(&host, "host", "127.0.0.1", "set host ip")

	flag.Parse()
}

func main() {
	parseFlags()

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

	schedule_server := rpc.NewScheduleServer(logger)

	schedule_server.Host = host
	schedule_server.Port = port

	if err := schedule_server.RemoteScheduleServer(); err != nil {
		log.Fatalln("failed to start schedule server", err.Error())
		return
	}
}
