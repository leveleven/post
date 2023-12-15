package rpc

import (
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

	"github.com/spacemeshos/post/config"
	"github.com/spacemeshos/post/internal/postrs"
	"github.com/spacemeshos/post/persistence"
	pb "github.com/spacemeshos/post/rpc/proto"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"google.golang.org/grpc"
)

const edKeyFileName = "key.bin"

var DefaultDataDir = filepath.Join("/data", "post")

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
	idHex              string
	id                 []byte
	commitmentAtxIdHex string
	numUnits           numUnitsFlag
	DataDir            string

	logLevel         zapcore.Level
	ErrKeyFileExists = errors.New("key file already exists")
)

func parseFlags() {
	flag.TextVar(&logLevel, "logLevel", zapcore.InfoLevel, "log level (debug, info, warn, error, dpanic, panic, fatal)")
	flag.StringVar(&DataDir, "datadir", DefaultDataDir, "filesystem datadir path")
	flag.StringVar(&idHex, "id", "", "miner's id (public key), in hex (will be auto-generated if not provided)")
	flag.StringVar(&commitmentAtxIdHex, "commitmentAtxId", "", "commitment atx id, in hex (required)")
	flag.Var(&numUnits, "numUnits", "number of units")
}

func nodeServer() {
	connect, err := grpc.Dial(":1234")
	if err != nil {
		log.Fatalln("Error connecting to server:", err)
		return
	}
	defer connect.Close()

	client := pb.NewPlotServiceClient(connect)

	if idHex == "" {
		pub, priv, err := ed25519.GenerateKey(nil)
		if err != nil {
			log.Fatalln("failed to generate identity: %w", err)
			return
		}
		id = pub
		log.Printf("cli: generated id %x\n", id)
		if err := saveKey(priv); err != nil {
			log.Fatalln("save key failed: ", err)
		}
	}
	// 这里拆分numUnits 做一个kv数据库
	var index = 0

	request := &pb.StreamRequest{
		IdHex:              idHex,
		CommitmentAtxIdHex: commitmentAtxIdHex,
		NumUnits:           numUnits.value,
		Index:              int64(index),
	}

	stream, err := client.Plot(context.Background(), nil)
	if err != nil {
		log.Fatalf("failed to open stream: %v", err)
	}

	if err := stream.Send(request); err != nil {
		log.Fatalf("failed to send message: %v", err)
	}

	saveFile(stream, index)

}

func saveFile(result pb.PlotService_PlotClient, index int) error {
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

	writer, err := persistence.NewLabelsWriter(DataDir, index, config.BitsPerLabel)
	if err != nil {
		// return err
	}
	defer writer.Close()

	for {
		res, err := result.Recv()
		if err != nil {
			log.Fatalf("failed to receive message: %v", err)
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
			logger.Debug("initialization: found nonce", fields...)

			// 判断全局nonce
			// if init.nonceValue.Load() == nil || bytes.Compare(candidate, *init.nonceValue.Load()) < 0 {
			// 	nonceValue := make([]byte, postrs.LabelLength)
			// 	copy(nonceValue, candidate)

			// 	logger.Info("initialization: found new best nonce", fields...)
			// 	init.nonce.Store(res.Nonce)
			// 	init.nonceValue.Store(&nonceValue)
			// 	init.saveMetadata()
			// }
		}

		// Write labels batch to disk.
		if err := writer.Write(res.Output); err != nil {
			return err
		}

		// numLabelsWritten.Store(res.FileOffset + res.CurrentPosition + uint64(batchSize))

		select {
		case <-result.Context().Done():
			break
		default:
		}
	}
}

func saveKey(key ed25519.PrivateKey) error {
	if err := os.MkdirAll(DataDir, 0o700); err != nil && !os.IsExist(err) {
		return fmt.Errorf("mkdir error: %w", err)
	}

	filename := filepath.Join(DataDir, edKeyFileName)
	if _, err := os.Stat(filename); err == nil {
		return ErrKeyFileExists
	}

	if err := os.WriteFile(filename, []byte(hex.EncodeToString(key)), 0o600); err != nil {
		return fmt.Errorf("key write to disk error: %w", err)
	}
	return nil
}
