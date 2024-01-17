package rpc

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spacemeshos/post/config"
	"github.com/spacemeshos/post/initialization"
	"github.com/spacemeshos/post/internal/postrs"
	"github.com/spacemeshos/post/persistence"
	pb "github.com/spacemeshos/post/rpc/proto"
	"github.com/spacemeshos/post/shared"
	"go.uber.org/zap"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
)

const (
	edKeyFileName = "key.bin"
	maxSize       = 20 * 1024 * 1024

	Pending = StatusType(0)
	Ploting = StatusType(1)
	Ploted  = StatusType(2)
)

var DefaultDataDir = filepath.Join("/data", "post")
var ErrReceiveDisconnect = errors.New("rpc server is disconnect")

type StatusType int

type NodeServer struct {
	Host     string
	Port     string
	Schedule string

	Node Node

	*pb.UnimplementedNodeServiceServer
}

type Node struct {
	nodeID          []byte
	CommitmentAtxId []byte
	NumUnits        uint32
	Nonces          []Nonce
	nonceValue      atomic.Pointer[[]byte]
	nonce           atomic.Pointer[uint64]
	lastPosition    atomic.Pointer[uint64]
	LabelsPerUnit   uint64

	Tasks     []*task
	Providers []*Provider

	Logger *zap.Logger
	Opts   *config.InitOpts
}

type task struct {
	index    int64
	provider Provider
	status   StatusType
	writer   *persistence.FileWriter
}

type Nonce struct {
	Nonce      *uint64
	NonceValue []byte
}

type PostData struct {
	Nonce uint
}

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

func (n *Node) saveFile(result pb.PlotService_PlotClient, task *task) error {
	var retry int
	for {
		res, err := result.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			time.Sleep(5 * time.Second)
			n.Logger.Error("failed to receive message", zap.String("error", err.Error()))
			// 超过次数返回重新获取GPU发任务
			retry += 1
			if retry == 3 {
				return ErrReceiveDisconnect
			}
			continue
		}
		retry = 0
		// res处理

		if res.Nonce != 0 {
			candidate := res.Output[(res.Nonce-res.StartPosition)*postrs.LabelLength:]
			candidate = candidate[:postrs.LabelLength]

			fields := []zap.Field{
				zap.Int64("fileIndex", task.index),
				zap.Uint64("nonce", res.Nonce),
				zap.String("value", hex.EncodeToString(candidate)),
			}
			n.Logger.Debug("nodeserver: found nonce", fields...)

			// 判断全局nonce
			nonce := Nonce{
				&res.Nonce,
				candidate,
			}
			n.Nonces = append(n.Nonces, nonce)
		}

		// Write labels batch to disk.
		if err := task.writer.Write(res.Output); err != nil {
			return err
		}
	}

	return nil
}

func (n *Node) saveKey(key ed25519.PrivateKey) error {
	if err := os.MkdirAll(n.Opts.DataDir, 0o700); err != nil && !os.IsExist(err) {
		return fmt.Errorf("mkdir error: %w", err)
	}

	filename := filepath.Join(n.Opts.DataDir, edKeyFileName)
	if err := os.WriteFile(filename, []byte(hex.EncodeToString(key)), 0o600); err != nil {
		return fmt.Errorf("key write to disk error: %w", err)
	}
	return nil
}

func (n *Node) getNodeID() ([]byte, error) {
	filename := filepath.Join(n.Opts.DataDir, edKeyFileName)
	if _, err := os.Stat(filename); err == nil {
		key, err := os.ReadFile(filename)
		if err != nil {
			return nil, fmt.Errorf("failed to open key file: %w", err)
		}
		key_string, _ := hex.DecodeString(string(key))
		n.Logger.Info("get node id", zap.String("id", hex.EncodeToString(key_string[32:])))
		return key_string[32:], nil
	}
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to generate identity: %w", err)
	}
	n.Logger.Info("nodeserver: generated node id", zap.ByteString("id", pub))
	if err := n.saveKey(priv); err != nil {
		return nil, fmt.Errorf("save key failed: %w", err)
	}
	return pub, nil
}

func (ns *NodeServer) GenerateTasks() error {
	id, err := ns.Node.getNodeID()
	if err != nil {
		return err
	}

	ns.Node.nodeID = id

	lastFileIndex := ns.Node.Opts.TotalFiles(ns.Node.LabelsPerUnit)
	ns.Node.Logger.Info("nodeserver: file infomation",
		zap.Uint32("numUnits", ns.Node.NumUnits),
		zap.Int("files", lastFileIndex),
		zap.Uint64("labelsPerUnit", ns.Node.LabelsPerUnit),
		zap.Uint64("maxFileSize", ns.Node.Opts.MaxFileSize),
	)
	for f := 0; f < lastFileIndex; f++ {
		ns.Node.Tasks = append(ns.Node.Tasks, &task{
			index:  int64(f),
			status: StatusType(0),
		})
	}

	return nil
}

func (n *Node) remotePlot(task *task) error {
	option := grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxSize))
	connect, err := grpc.Dial(task.provider.Host+":"+task.provider.Port, grpc.WithTransportCredentials(insecure.NewCredentials()), option)
	if err != nil {
		return fmt.Errorf("error connecting to server: %w", err)
	}
	defer connect.Close()

	// 获取connect(最大值判断)
	client := pb.NewPlotServiceClient(connect)

	// 打开文件
	task.writer, err = persistence.NewLabelsWriter(n.Opts.DataDir, int(task.index), config.BitsPerLabel)
	if err != nil {
		return err
	}
	defer task.writer.Close()
	numLabelsWritten, err := task.writer.NumLabelsWritten()
	if err != nil {
		return err
	}

	fileNumLabels := n.Opts.MaxFileNumLabels()

	fields := []zap.Field{
		zap.Int64("fileIndex", task.index),
		zap.Uint64("currentNumLabels", numLabelsWritten),
		zap.Uint64("targetNumLabels", fileNumLabels),
	}
	switch {
	case numLabelsWritten == fileNumLabels:
		n.Logger.Info("nodeserver: file already initialized", fields...)
		return nil
	case numLabelsWritten > fileNumLabels:
		n.Logger.Info("nodeserver: truncating file")
		if err := task.writer.Truncate(fileNumLabels); err != nil {
			return err
		}
		return nil
	case numLabelsWritten > 0:
		n.Logger.Info("nodeserver: continuing to write file", fields...)
	default:
		n.Logger.Info("nodeserver: starting to write file", fields...)
	}

	request := &pb.Task{
		Id:               n.nodeID,
		CommitmentAtxId:  n.CommitmentAtxId,
		NumUnits:         n.NumUnits,
		Index:            task.index,
		LabelsPerUnit:    n.LabelsPerUnit,
		MaxFileSize:      n.Opts.MaxFileSize,
		NumLabelsWritten: numLabelsWritten,
		Provider: &pb.Provider{
			ID:    task.provider.ID,
			Model: task.provider.Model,
			UUID:  task.provider.UUID,
		},
	}

	stream, err := client.Plot(context.Background(), request)
	if err != nil {
		return fmt.Errorf("failed to open stream: %v", err)
	}

	task.status = StatusType(1)
	if err := n.saveFile(stream, task); err != nil {
		fields := []zap.Field{
			zap.Int64("index:", task.index),
			zap.String("error:", err.Error()),
		}
		n.Logger.Error("ploting failed:", fields...)
		task.status = StatusType(0)
		return err
	}
	task.status = StatusType(3)
	return nil
}

func (ns *NodeServer) getProvider(client pb.ScheduleServiceClient) (*pb.Provider, error) {
	for {
		provider, err := client.GetFreeProvider(context.Background(), &pb.Empty{})
		if err != nil {
			return &pb.Provider{}, fmt.Errorf("failed to call method: %w", err)
		}
		if provider.UUID == "" {
			time.Sleep(5 * time.Second)
			continue
		}
		return provider, nil
	}
}

func (ns *NodeServer) plot(id int, tasks chan *task, wg *sync.WaitGroup) {
	defer wg.Done()

	schedule, err := grpc.Dial(ns.Schedule, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		ns.Node.Logger.Error("Error connecting to schedule server", zap.Error(err))
	}
	client := pb.NewScheduleServiceClient(schedule)
	defer schedule.Close()

	for task := range tasks {
		// 获取provider
		ns.Node.Logger.Info("Get task",
			zap.Int("worker_id", id),
			zap.Int64("plot_index", task.index))

		var provider *pb.Provider

		for {
			provider, err = ns.getProvider(client)
			if err != nil {
				ns.Node.Logger.Error("Failed to get provider", zap.Error(err))
				tasks <- task
			}
			task.provider = Provider{
				ID:     provider.ID,
				Model:  provider.Model,
				UUID:   provider.UUID,
				Host:   provider.Host,
				Port:   provider.Port,
				Status: GPUStatusType(provider.Status),
			}

			// 获取provider connect
			ns.Node.Logger.Info("nodeserver: get provider",
				zap.String("uuid", task.provider.UUID),
				zap.String("host", task.provider.Host+":"+task.provider.Port),
				zap.String("model", task.provider.Model),
			)

			ns.Node.Logger.Info("Start plotting:",
				zap.Int("Worker", id),
				zap.Int64("Index", task.index),
			)

			// 中断续传
			if err = ns.Node.remotePlot(task); err != nil {
				ns.Node.Logger.Warn("nodeserver: remote provider offline",
					zap.Int64("index", task.index),
					zap.String("provider uuid", task.provider.UUID),
				)
				if err = changeProviderStatus(context.Background(), client, provider.UUID, -1); err != nil {
					ns.Node.Logger.Error("nodeserver: schedule rpc server", zap.Error(err))
				}
				continue
			}
			break
		}

		ns.Node.Logger.Info("Finish plot:",
			zap.Int("Worker", id),
			zap.Int64("Index", task.index),
		)

		if err = changeProviderStatus(context.Background(), client, provider.UUID, 0); err != nil {
			ns.Node.Logger.Error("nodeserver: schedule rpc server", zap.Error(err))
		}
	}
}

func changeProviderStatus(ctx context.Context, sc pb.ScheduleServiceClient, uuid string, status int32) error {
	_, err := sc.ChangeProviderStatus(context.Background(), &pb.Pstatus{UUID: uuid, Status: status})
	if err != nil {
		return fmt.Errorf("failed to call method: %w", err)
	}
	return nil
}

func (ns *NodeServer) StartPlot(parallel int) {
	n := &ns.Node

	tasks := make(chan *task, len(n.Tasks))
	var wg sync.WaitGroup
	for w := 0; w < parallel; w++ {
		wg.Add(1)
		go ns.plot(w, tasks, &wg)
	}

	for _, t := range n.Tasks {
		tasks <- t
	}
	close(tasks)
	wg.Wait()

	// 文件全部做完，开始比较nonce
	for _, nes := range n.Nonces {
		fields := []zap.Field{
			zap.Uint64("nonce", *nes.Nonce),
			zap.String("nonceValue", hex.EncodeToString(nes.NonceValue)),
		}
		n.Logger.Debug("nodeserver: get nonces", fields...)
		if n.nonceValue.Load() == nil || bytes.Compare(nes.NonceValue, *n.nonceValue.Load()) < 0 {
			nonceValue := make([]byte, postrs.LabelLength)
			copy(nonceValue, nes.NonceValue)

			n.Logger.Info("nodeserver: found new best nonce", fields...)
			n.nonce.Store(nes.Nonce)
			n.nonceValue.Store(&nonceValue)
		}
	}

	if n.nonce.Load() != nil {
		n.Logger.Info("nodeserver: completed, found nonce",
			zap.Uint64("nonce", *n.nonce.Load()),
			zap.String("nonceValue", hex.EncodeToString(*n.nonceValue.Load())),
		)
	}
	defer n.saveMetadata()
}

func (n *Node) saveMetadata() error {
	v := shared.PostMetadata{
		NodeId:          n.nodeID,
		CommitmentAtxId: n.CommitmentAtxId,
		LabelsPerUnit:   n.LabelsPerUnit,
		NumUnits:        n.NumUnits,
		MaxFileSize:     n.Opts.MaxFileSize,
		Nonce:           n.nonce.Load(),
		LastPosition:    n.lastPosition.Load(), // 定位未做完的bin
	}
	if n.nonceValue.Load() != nil {
		v.NonceValue = *n.nonceValue.Load()
	}
	return initialization.SaveMetadata(n.Opts.DataDir, &v)
}

func (ns *NodeServer) RemoteNodeServer() error {
	listener, err := net.Listen("tcp", ns.Host+":"+ns.Port)
	if err != nil {
		return fmt.Errorf("failed to listen "+ns.Host+":"+ns.Port+" ", err)
	}

	rps := grpc.NewServer()
	reflection.Register(rps)
	pb.RegisterNodeServiceServer(rps, ns)
	ns.Node.Logger.Info("Node server is listening on " + ns.Host + ":" + ns.Port)
	if err := rps.Serve(listener); err != nil {
		return fmt.Errorf("failed to serve: %w", err)
	}
	return nil
}

func (ns *NodeServer) ShowTasks(ctx context.Context, empty *pb.Empty) (*pb.Tasks, error) {
	var tasks []*pb.Task
	for _, value := range ns.Node.Tasks {
		tasks = append(tasks, &pb.Task{
			Id:              ns.Node.nodeID,
			CommitmentAtxId: ns.Node.CommitmentAtxId,
			NumUnits:        ns.Node.NumUnits,
			Index:           value.index,
			Provider: &pb.Provider{
				ID:    value.provider.ID,
				Model: value.provider.Model,
				UUID:  value.provider.UUID,
			},
			Status: value.status.String(),
		})
	}
	return &pb.Tasks{Tasks: tasks}, nil
}
