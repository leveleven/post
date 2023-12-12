package server

import (
	"encoding/hex"
	"fmt"
	"log"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/spacemeshos/post/initialization"
)

// 任务信息
// commitmentAtx
// indexFile
// numUnits
// MaxFileSize ?
// LabelsPerUnit ?

func server() {
	r := gin.Default()
	r.POST("/worker", func(ctx *gin.Context) {
		obj := struct {
			CommitmentAtxIdHex string `json:"commitmentAtxIdHex"`
			IndexFile          int    `json:"indexFile"`
			NumUnits           int    `json:"numUnits"`
		}{}
		// 进入p盘，返回机器状态
	})

	r.Run()
}

func runInitialization(id []byte, commitmentAtxIdHex string) error {
	commitmentAtxId, err := hex.DecodeString(commitmentAtxIdHex)
	if err != nil {
		return fmt.Errorf("invalid commitmentAtxId: %w", err)
	}

	var logLevel zapcore.Level
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
}
