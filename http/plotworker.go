package http

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/spacemeshos/post/config"
	"github.com/spacemeshos/post/initialization"
	"github.com/spacemeshos/post/internal/postrs"
	"github.com/spacemeshos/post/shared"
)

// 任务信息
// commitmentAtx
// indexFile
// numUnits
// MaxFileSize ?
// LabelsPerUnit ?
type InitOption struct {
	IDHex              string `json:"id"`
	CommitmentAtxIdHex string `json:"commitmentAtxId"`
	NumUnits           uint32 `json:"numUnits"`
	Index              int    `json:"index"`
	DataDir            string
	Provider           postrs.Provider
}

type Provider struct {
	postrs.Provider
	Use bool
}

type Fetch struct {
	Fetched *http.Server
	Port    int
	Host    string
}

var BaseDir = "/data/"
var providers, _ = postrs.OpenCLProviders()
var gpu_providers = make([]Provider, len(providers)-1)

func server() {
	worker := make(chan postrs.Provider, len(providers)-1)
	exits_ignal := make(chan os.Signal)

	for _, provider := range providers {
		if provider.DeviceType == 2 {
			var gpu_provider Provider
			gpu_provider.Use = false
			gpu_providers = append(gpu_providers, gpu_provider)
			worker <- provider
		}
	}

	r := gin.Default()
	r.POST("/worker", func(ctx *gin.Context) {
		io := &InitOption{}
		err := ctx.ShouldBind(io)
		if err != nil {
			ctx.JSON(500, gin.H{
				"msg": err.Error(),
			})
		}
		// 进入p盘，返回机器状态
		io.Provider = <-worker
		go func() {
			changeProviderInUse(gpu_providers, io.Provider.ID, true)
			io.DataDir = filepath.Join(BaseDir, uuid.NewString())
			server, err := io.Running(worker)
			if err != nil {
				panic(err)
			}
			var t = &Fetch{
				Fetched: server,
			}
			// 如何构建文件服务器并在完成传输时关闭文件服务器：
			// 方案一：在完成init时，与调度进行交互，请求体包含对应ip、plot文件、队列key等信息，传输完成时调度再请求plotnode关闭文件服务
			t.SeverFile(exits_ignal)
		}()
		ctx.JSON(200, gin.H{
			"status": "running",
			"id":     io.Provider.ID,
			"model":  io.Provider.Model,
		})
	})

	r.GET("/worker", func(ctx *gin.Context) {
		ctx.JSON(200, gpu_providers)
	})

	r.Run()
}

func changeProviderInUse(providers []Provider, provider uint32, use bool) {
	for index, gpu := range providers {
		if gpu.ID == provider {
			providers[index].Use = use
			break
		}
	}
}

func (t *Fetch) SeverFile(signal chan os.Signal) {
	ctx := context.Background()
	t.Fetched.ListenAndServe()
	select {
	case <-signal:
		t.Fetched.Shutdown(ctx)
	case <-ctx.Done():
	default:
	}
}

func (io *InitOption) Running(worker chan postrs.Provider) (*http.Server, error) {
	var logLevel zapcore.Level
	var cfg = config.MainnetConfig()
	var opts = config.MainnetInitOpts()

	commitmentAtxId, err := hex.DecodeString(io.CommitmentAtxIdHex)
	if err != nil {
		return nil, fmt.Errorf("invalid commitmentAtxId: %w", err)
	}
	id, err := hex.DecodeString(io.IDHex)
	if err != nil {
		return nil, fmt.Errorf("invalid id: %w", err)
	}
	opts.FromFileIdx = io.Index
	opts.ToFileIdx = &io.Index
	opts.NumUnits = io.NumUnits
	opts.ProviderID = &io.Provider.ID
	opts.DataDir = io.DataDir

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

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

	err = init.SingleInitialize(ctx)
	switch {
	case errors.Is(err, shared.ErrInitCompleted):
		return nil, err
	case errors.Is(err, context.Canceled):
		return nil, fmt.Errorf("cli: initialization interrupted")
	case err != nil:
		return nil, fmt.Errorf("cli: initialization error", err)
	}

	log.Println("cli: initialization completed")

	worker <- io.Provider
	changeProviderInUse(gpu_providers, io.Provider.ID, false)

	// 完成开启回传端口

	// 发送消息到接收端等待下载

	// 下载完成发送信号到发送端结束文件服务器
	port := make(chan int)
	for {
		randomPort := rand.Intn(100)
		if checkPort(randomPort) {
			port <- randomPort
			break
		}
	}

	return &http.Server{
		Addr:    fmt.Sprintf(":20%s", port),
		Handler: http.HandlerFunc(io.Fetch),
	}, nil
}

func checkPort(port int) bool {
	listen, err := net.Listen("tcp", fmt.Sprintf(":20%s", port))
	if err != nil {
		return false
	}
	defer listen.Close()
	return true
}

func (io *InitOption) Fetch(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, io.DataDir)
}
