package main

import (
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"log"
	"path/filepath"
	"strconv"

	"github.com/spacemeshos/post/config"
	"github.com/spacemeshos/post/rpc"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var DefaultDataDir = filepath.Join("/data", "post")

type numUnitsFlag struct {
	set   bool
	value uint32
}

func (nu *numUnitsFlag) Set(s string) error {
	val, err := strconv.ParseUint(s, 10, 32)
	if err != nil {
		return err
	}
	*nu = numUnitsFlag{
		set:   true,
		value: uint32(val),
	}
	return nil
}

func (nu *numUnitsFlag) String() string {
	return fmt.Sprintf("%d", nu.value)
}

var (
	opts = config.MainnetInitOpts()

	host     string
	port     string
	schedule string
	parallel int

	idHex              string
	id                 []byte
	commitmentAtxIdHex string
	commitmentAtxId    []byte
	numUnits           numUnitsFlag
	labelsPerUnit      uint64

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
	flag.IntVar(&parallel, "parallel", 40, "parallel plot number, depend on your disk bandwidth")

	flag.Uint64Var(&opts.MaxFileSize, "maxFileSize", opts.MaxFileSize, "max file size")
	flag.Uint64Var(&labelsPerUnit, "labelsPerUnit", config.MainnetConfig().LabelsPerUnit, "the number of labels per unit")

	flag.Parse()

	if numUnits.set {
		opts.NumUnits = numUnits.value
	}
}

func processFlags() error {
	// we require the user to explicitly pass numunits to avoid erasing existing data
	if !numUnits.set {
		return fmt.Errorf("-numUnits must be specified to perform initialization. to use the default value, "+
			"run with -numUnits %d. note: if there's more than this amount of data on disk, "+
			"THIS WILL ERASE EXISTING DATA. MAKE ABSOLUTELY SURE YOU SPECIFY THE CORRECT VALUE", opts.NumUnits)
	}

	if commitmentAtxIdHex == "" {
		return errors.New("-commitmentAtxId flag is required")
	}
	var err error
	commitmentAtxId, err = hex.DecodeString(commitmentAtxIdHex)
	if err != nil {
		return fmt.Errorf("invalid commitmentAtxId: %w", err)
	}
	return nil
}

func main() {
	parseFlags()

	var err error
	if err = processFlags(); err != nil {
		log.Fatalln("failed to process flags:", err)
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

	node_server := rpc.NodeServer{
		Host:     host,
		Port:     port,
		Schedule: schedule,
		Node: rpc.Node{
			CommitmentAtxId: commitmentAtxId,
			NumUnits:        numUnits.value,
			LabelsPerUnit:   labelsPerUnit,
			Opts:            &opts,
			Logger:          logger,
		},
	}

	// 加载任务
	if err = node_server.GenerateTasks(); err != nil {
		log.Fatalln("failed to generate tasks:", err)
	}

	// 启动plot
	// if err = node_server.StartPlot(parallel); err != nil {
	// 	log.Fatalln("failed to start plot:", err)
	// }
	go node_server.StartPlot(parallel)

	// 启动服务
	if err = node_server.RemoteNodeServer(); err != nil {
		log.Fatalln("failed to start server:", err)
	}
}
