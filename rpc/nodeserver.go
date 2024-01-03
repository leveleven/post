package rpc

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

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

const edKeyFileName = "key.bin"

const (
	Pending = StatusType(0)
	Ploting = StatusType(1)
	Ploted  = StatusType(2)
)

var DefaultDataDir = filepath.Join("/data", "post")

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

	Tasks     []*Task
	Providers []*Provider

	Logger *zap.Logger
	Opts   *config.InitOpts
}

type Task struct {
	Index    int64
	Provider Provider
	Status   StatusType
}

type Nonce struct {
	Nonce      uint64
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

func (n *Node) saveFile(result pb.PlotService_PlotClient, index int) error {

	writer, err := persistence.NewLabelsWriter(n.Opts.DataDir, index, config.BitsPerLabel)
	if err != nil {
		return err
	}
	defer writer.Close()

loop:
	for {
		res, err := result.Recv()
		if err != nil {
			n.Logger.Error("failed to receive message: %v", zap.String("error", err.Error()))
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
			n.Logger.Debug("initialization: found nonce", fields...)

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
	n.Logger.Info("generated node id", zap.ByteString("id", pub))
	if err := n.saveKey(priv); err != nil {
		return nil, fmt.Errorf("save key failed: ", err)
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
	ns.Node.Logger.Info("file infomation",
		zap.Uint32("numUnits", ns.Node.NumUnits),
		zap.Int("files", lastFileIndex),
		zap.Uint64("labelsPerUnit", ns.Node.LabelsPerUnit),
		zap.Uint64("maxFileSize", ns.Node.Opts.MaxFileSize),
	)
	for f := 0; f < lastFileIndex; f++ {
		ns.Node.Tasks = append(ns.Node.Tasks, &Task{
			Index:  int64(f),
			Status: StatusType(0),
		})
	}

	return nil
}

func (n *Node) remotePlot(task *Task, connect *grpc.ClientConn) {
	defer connect.Close()

	// 获取connect(最大值判断)
	client := pb.NewPlotServiceClient(connect)
	request := &pb.Task{
		Id:              n.nodeID,
		CommitmentAtxId: n.CommitmentAtxId,
		NumUnits:        n.NumUnits,
		Index:           task.Index,
		Provider: &pb.Provider{
			ID:    task.Provider.ID,
			Model: task.Provider.Model,
			UUID:  task.Provider.UUID,
		},
	}

	stream, err := client.Plot(context.Background(), request)
	if err != nil {
		log.Fatalf("failed to open stream: %v", err)
	}

	task.Status = StatusType(1)
	if err := n.saveFile(stream, int(task.Index)); err != nil {
		fields := []zap.Field{
			zap.Int64("index:", task.Index),
			zap.String("error:", err.Error()),
		}
		n.Logger.Error("ploting failed:", fields...)
		task.Status = StatusType(0)
		return
	}
	task.Status = StatusType(3)
}

func (ns *NodeServer) plot(id int, tasks chan *Task, wg *sync.WaitGroup, errCh chan error) {
	defer wg.Done()

	for task := range tasks {
		ns.Node.Logger.Info("Start plotting:",
			zap.Int("Worker", id),
			zap.Int64("Index", task.Index),
		)
		schedule, err := grpc.Dial(ns.Schedule, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			err = fmt.Errorf("Error connecting to schedule server:", err)
			tasks <- task
			errCh <- err
			return
		}
		client := pb.NewScheduleServiceClient(schedule)
		// 获取worker
		provider, err := client.GetFreeProvider(context.Background(), &pb.Empty{})
		if err != nil {
			ns.Node.Logger.Error("Failed to call method", zap.Error(err))
			tasks <- task
			return
		}

		// 获取provider connect
		connect, err := grpc.Dial(provider.Host + ":" + provider.Port)
		if err != nil {
			ns.Node.Logger.Error("Error connecting to server:", zap.Error(err))
			tasks <- task
			return
		}

		ns.Node.remotePlot(task, connect)
	}
}

func (ns *NodeServer) StartPlot(parallel int) error {
	n := &ns.Node

	tasks := make(chan *Task, len(n.Tasks))
	errCh := make(chan error)
	var wg sync.WaitGroup
	for w := 0; w < parallel; w++ {
		wg.Add(1)
		go ns.plot(w, tasks, &wg, errCh)
		select {
		case err := <-errCh:
			if err != nil {
				return err
			}
		}
	}

	for _, t := range n.Tasks {
		tasks <- t
	}
	close(tasks)
	wg.Wait()

	// 文件全部做完，开始比较nonce
	n.nonce.Store(&n.Nonces[0].Nonce)
	n.nonceValue.Store(&n.Nonces[0].NonceValue)
	for _, nes := range n.Nonces {
		n.Logger.Debug("Get nonces:", zap.Binary("nonceValue", nes.NonceValue))
		if bytes.Compare(*n.nonceValue.Load(), nes.NonceValue) > 0 {
			n.nonce.Store(&nes.Nonce)
			n.nonceValue.Store(&nes.NonceValue)
		}
	}

	if n.nonce.Load() != nil {
		n.Logger.Info("initialization: completed, found nonce", zap.Uint64("nonce", *n.nonce.Load()))
	}
	defer n.saveMetadata()

	return nil
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
	fmt.Println("Node server is listening on " + ns.Host + ":" + ns.Port)
	if err := rps.Serve(listener); err != nil {
		return fmt.Errorf("failed to serve:", err)
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
			Index:           value.Index,
			Provider: &pb.Provider{
				ID:    value.Provider.ID,
				Model: value.Provider.Model,
				UUID:  value.Provider.UUID,
			},
			Status: value.Status.String(),
		})
	}
	return &pb.Tasks{Tasks: tasks}, nil
}
