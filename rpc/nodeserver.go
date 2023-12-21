package rpc

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"

	"github.com/spacemeshos/post/config"
	"github.com/spacemeshos/post/initialization"
	"github.com/spacemeshos/post/internal/postrs"
	"github.com/spacemeshos/post/persistence"
	pb "github.com/spacemeshos/post/rpc/proto"
	"github.com/spacemeshos/post/shared"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"google.golang.org/grpc"
)

const edKeyFileName = "key.bin"

var DefaultDataDir = filepath.Join("/data", "post")

type Node struct {
	ID              []byte
	CommitmentAtxId []byte
	NumUnits        numUnitsFlag
	// DataDir         string
	Nonces       []Nonce
	nonceValue   atomic.Pointer[[]byte]
	nonce        atomic.Pointer[uint64]
	lastPosition atomic.Pointer[uint64]
	Provider     uint32

	logger *zap.Logger
	opts   *config.InitOpts
}

type Nonce struct {
	Nonce      uint64
	NonceValue []byte
}

type PostData struct {
	Nonce uint
}

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

	idHex              string
	id                 []byte
	commitmentAtxIdHex string
	numUnits           numUnitsFlag
	parallel           int
	// DataDir            string

	logLevel zapcore.Level

	// MaxFileSize   uint64
	LabelsPerUnit uint64

	ErrKeyFileExists = errors.New("key file already exists")
)

func parseFlags() {
	flag.TextVar(&logLevel, "logLevel", zapcore.InfoLevel, "log level (debug, info, warn, error, dpanic, panic, fatal)")
	flag.StringVar(&opts.DataDir, "datadir", DefaultDataDir, "filesystem datadir path")
	flag.StringVar(&idHex, "id", "", "miner's id (public key), in hex (will be auto-generated if not provided)")
	flag.StringVar(&commitmentAtxIdHex, "commitmentAtxId", "", "commitment atx id, in hex (required)")
	flag.Var(&numUnits, "numUnits", "number of units")
	flag.IntVar(&parallel, "parallel", 40, "parallel plot number, depend on your disk bandwidth (default 40)")

	flag.Uint64Var(&opts.MaxFileSize, "maxFileSize", config.MainnetInitOpts().MaxFileSize, "max file size")
	flag.Uint64Var(&LabelsPerUnit, "labelsPerUnit", config.MainnetConfig().LabelsPerUnit, "the number of labels per unit")

	flag.Parse()
}

func (n *Node) saveFile(result pb.PlotService_PlotClient, index int) error {

	writer, err := persistence.NewLabelsWriter(n.opts.DataDir, index, config.BitsPerLabel)
	if err != nil {
		return err
	}
	defer writer.Close()

loop:
	for {
		res, err := result.Recv()
		if err != nil {
			n.logger.Error("failed to receive message: %v", zap.String("error", err.Error()))
			continue
		}
		// res处理
		if res.Nonce != 0 {
			candidate := res.Output[(res.Nonce-res.StartPosition)*postrs.LabelLength:]
			candidate = candidate[:postrs.LabelLength]

			fields := []zap.Field{
				zap.Int("fileIndex", index),
				zap.Uint64("nonce", res.Nonce),
				zap.String("value", hex.EncodeToString(candidate)),
			}
			n.logger.Debug("initialization: found nonce", fields...)

			// 判断全局nonce
			nonce := Nonce{
				res.Nonce,
				candidate,
			}
			n.Nonces = append(n.Nonces, nonce)
		}

		// Write labels batch to disk.
		if err := writer.Write(res.Output); err != nil {
			return err
		}

		// numLabelsWritten.Store(res.FileOffset + res.CurrentPosition + uint64(batchSize))

		select {
		case <-result.Context().Done():
			break loop
		default:
		}
	}

	return nil
}

func (n *Node) saveKey(key ed25519.PrivateKey) error {
	if err := os.MkdirAll(n.opts.DataDir, 0o700); err != nil && !os.IsExist(err) {
		return fmt.Errorf("mkdir error: %w", err)
	}

	filename := filepath.Join(n.opts.DataDir, edKeyFileName)
	if _, err := os.Stat(filename); err == nil {
		return ErrKeyFileExists
	}

	if err := os.WriteFile(filename, []byte(hex.EncodeToString(key)), 0o600); err != nil {
		return fmt.Errorf("key write to disk error: %w", err)
	}
	return nil
}

type StatusType int

func (n *Node) generateFileStatus(lastFileIndex int) map[int]StatusType {
	fileStatus := make(map[int]StatusType)
	for i := 0; i < lastFileIndex; i++ {
		fileStatus[i] = StatusType(0)
	}
	return fileStatus
}

const (
	Pending = StatusType(0)
	Ploting = StatusType(1)
	Ploted  = StatusType(2)
)

func (s StatusType) String() string {
	switch s {
	case Pending:
		return "pending"
	case Ploting:
		return "ploting"
	case Ploted:
		return "ploted"
	default:
		return "unknown"
	}
}

func newNode() (*Node, error) {
	var node Node

	if idHex == "" {
		pub, priv, err := ed25519.GenerateKey(nil)
		if err != nil {
			return nil, fmt.Errorf("failed to generate identity: %w", err)

		}
		id = pub
		log.Printf("cli: generated id %x\n", id)
		if err := node.saveKey(priv); err != nil {
			return nil, fmt.Errorf("save key failed: ", err)
		}
	}

	commitmentAtxId, err := hex.DecodeString(commitmentAtxIdHex)
	if err != nil {
		return nil, fmt.Errorf("invalid commitmentAtxId: %w", err)
	}
	id, err := hex.DecodeString(idHex)
	if err != nil {
		return nil, fmt.Errorf("invalid id: %w", err)
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
		log.Fatalln("failed to inuint64itialize zap logger:", err)
	}

	node.ID = id
	node.CommitmentAtxId = commitmentAtxId
	node.NumUnits = numUnits
	// node.DataDir = opts.DataDir
	node.logger = logger
	node.opts = &opts

	return &node, nil
}

func (n *Node) remotePlot(index int, fileStatus map[int]StatusType, connect *grpc.ClientConn) {
	defer connect.Close()

	// 获取connect(最大值判断)
	client := pb.NewPlotServiceClient(connect)
	request := &pb.StreamRequest{
		Id:              n.ID,
		CommitmentAtxId: n.CommitmentAtxId,
		NumUnits:        n.NumUnits.value,
		Index:           int64(index),
		Provider:        n.Provider,
	}

	stream, err := client.Plot(context.Background(), nil)
	if err != nil {
		log.Fatalf("failed to open stream: %v", err)
	}

	if err := stream.Send(request); err != nil {
		log.Fatalf("failed to send message: %v", err)
	}
	fileStatus[index] = StatusType(1)
	if err := n.saveFile(stream, index); err != nil {
		fields := []zap.Field{
			zap.Int("index:", index),
			zap.String("error:", err.Error()),
		}
		n.logger.Error("ploting failed:", fields...)
		fileStatus[index] = StatusType(0)
		return
	}
	fileStatus[index] = StatusType(3)
}

func NodeServer() {
	parseFlags()
	node, _ := newNode()

	// 这里拆分numUnits 做一个kv数据库 自己管理任务状态
	// 1. 列出所有需要做的文件
	// 2. 获取所需要的provider
	// 3. 进入工作
	lastFileIndex := opts.TotalFiles(LabelsPerUnit)
	fileStatus := node.generateFileStatus(lastFileIndex)

	for f := 0; f < lastFileIndex; f++ {
		// 获取provider connect
		// 获取worker

		connect, err := grpc.Dial("10.100.85.0:1234")
		if err != nil {
			log.Fatalln("Error connecting to server:", err)
			return
		}
		node.remotePlot(f, fileStatus, connect)
	}

	// 文件全部做完，开始比较nonce
	node.nonce.Store(&node.Nonces[0].Nonce)
	node.nonceValue.Store(&node.Nonces[0].NonceValue)
	for _, n := range node.Nonces {
		if bytes.Compare(*node.nonceValue.Load(), n.NonceValue) > 0 {
			node.nonce.Store(&n.Nonce)
			node.nonceValue.Store(&n.NonceValue)

		}
	}

	if node.nonce.Load() != nil {
		node.logger.Info("initialization: completed, found nonce", zap.Uint64("nonce", *node.nonce.Load()))
		return
	}

	defer node.saveMetadata()
}

func (n *Node) saveMetadata() error {
	v := shared.PostMetadata{
		NodeId:          n.ID,
		CommitmentAtxId: n.CommitmentAtxId,
		LabelsPerUnit:   LabelsPerUnit,
		NumUnits:        n.NumUnits.value,
		MaxFileSize:     n.opts.MaxFileSize,
		Nonce:           n.nonce.Load(),
		LastPosition:    n.lastPosition.Load(), // 定位未做完的bin
	}
	if n.nonceValue.Load() != nil {
		v.NonceValue = *n.nonceValue.Load()
	}
	return initialization.SaveMetadata(n.opts.DataDir, &v)
}
