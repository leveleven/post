package initialization

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/spacemeshos/post/config"
	"github.com/spacemeshos/post/internal/postrs"
	"github.com/spacemeshos/post/oracle"
	"github.com/spacemeshos/post/shared"
	"go.uber.org/zap"

	pb "github.com/spacemeshos/post/rpc/proto"
)

// Initializer is responsible for initializing a new PoST commitment.
type InitializerSingle struct {
	nodeId          []byte
	commitmentAtxId []byte
	index           int64

	cfg  Config
	opts InitOpts

	// these values are atomics so they can be read from multiple other goroutines safely
	// write is protected by mtx
	// nonceValue   atomic.Pointer[[]byte]
	// nonce        atomic.Pointer[uint64]
	// lastPosition atomic.Pointer[uint64]
	numLabelsWritten atomic.Uint64

	diskState *DiskState
	mtx       sync.RWMutex // TODO(mafa): instead of a RWMutex we should lock with a lock file to prevent other processes from initializing/modifying the data

	logger            *Logger
	referenceOracle   *oracle.WorkOracle
	powDifficultyFunc func(uint64) []byte
}

func NewSingleInitializer(opts ...OptionFunc) (*InitializerSingle, error) {
	options := &option{
		logger:            zap.NewNop(),
		powDifficultyFunc: shared.PowDifficulty,
	}

	for _, opt := range opts {
		if err := opt(options); err != nil {
			return nil, err
		}
	}

	if err := options.validate(); err != nil {
		return nil, err
	}

	init := &InitializerSingle{
		cfg:               *options.cfg,
		opts:              *options.initOpts,
		nodeId:            options.nodeId,
		commitmentAtxId:   options.commitmentAtxId,
		diskState:         NewDiskState(options.initOpts.DataDir, uint(config.BitsPerLabel)),
		logger:            options.logger,
		powDifficultyFunc: options.powDifficultyFunc,
		referenceOracle:   options.referenceOracle,
		index:             options.index,
	}
	init.numLabelsWritten.Store(options.numLabelsWritten)

	return init, nil
}

func (init *InitializerSingle) SingleInitialize(provider *uint32, stream pb.PlotService_PlotServer) error {
	if !init.mtx.TryLock() {
		return ErrAlreadyInitializing
	}
	defer init.mtx.Unlock()

	layout, err := deriveFilesLayout(init.cfg, init.opts)
	if err != nil {
		return err
	}
	index := init.index
	init.logger.Info("initialization started",
		zap.String("datadir", init.opts.DataDir),
		zap.Uint32("numUnits", init.opts.NumUnits),
		zap.Uint64("maxFileSize", init.opts.MaxFileSize),
		zap.Uint64("labelsPerUnit", init.cfg.LabelsPerUnit),
	)

	init.logger.Info("initialization file layout",
		zap.Uint64("labelsPerFile", layout.FileNumLabels),
		zap.Uint64("labelsLastFile", layout.LastFileNumLabels),
		zap.Int64("FileIndex", index),
	)

	numLabels := uint64(init.opts.NumUnits) * init.cfg.LabelsPerUnit
	difficulty := init.powDifficultyFunc(numLabels)
	batchSize := init.opts.ComputeBatchSize

	wo, err := oracle.New(
		oracle.WithProviderID(provider),
		oracle.WithCommitment(init.commitmentAtxId),
		oracle.WithVRFDifficulty(difficulty),
		oracle.WithScryptParams(init.opts.Scrypt),
		oracle.WithLogger(init.logger),
	)
	if err != nil {
		return err
	}
	defer wo.Close()

	woReference := init.referenceOracle
	if woReference == nil {
		cpuProvider := CPUProviderID()
		woReference, err = oracle.New(
			oracle.WithProviderID(&cpuProvider),
			oracle.WithCommitment(init.commitmentAtxId),
			oracle.WithVRFDifficulty(difficulty),
			oracle.WithScryptParams(init.opts.Scrypt),
			oracle.WithLogger(init.logger),
		)
		if err != nil {
			return err
		}
		defer woReference.Close()
	}

	fileOffset := uint64(index) * layout.FileNumLabels
	fileNumLabels := layout.FileNumLabels
	if err := init.initSingleFile(stream, wo, woReference, index, batchSize, fileOffset, fileNumLabels, difficulty); err != nil {
		return err
	}

	stream.Context().Done()

	return nil
}

func (init *InitializerSingle) initSingleFile(stream pb.PlotService_PlotServer, wo, woReference *oracle.WorkOracle, fileIndex int64, batchSize, fileOffset, fileNumLabels uint64, difficulty []byte) error {
	// fileTargetPosition := fileOffset + fileNumLabels
	numLabelsWritten := init.numLabelsWritten.Load()
	fields := []zap.Field{
		zap.Int64("fileIndex", fileIndex),
		zap.Uint64("currentNumLabels", numLabelsWritten),
		zap.Uint64("targetNumLabels", fileNumLabels),
		zap.Uint64("startPosition", fileOffset),
	}
	init.logger.Info("initialization: starting to plot file", fields...)

	// 断点续做 numLabelsWritten
	for currentPosition := numLabelsWritten; currentPosition < fileNumLabels; currentPosition += batchSize {
		// The last batch might need to be smaller.
		remaining := fileNumLabels - currentPosition
		if remaining < batchSize {
			batchSize = remaining
		}

		init.logger.Debug("initialization: status",
			zap.Int64("fileIndex", fileIndex),
			zap.Uint64("currentPosition", currentPosition),
			zap.Uint64("remaining", remaining),
		)

		// Calculate labels of the batch position range.
		startPosition := fileOffset + currentPosition
		endPosition := startPosition + uint64(batchSize) - 1
		res, err := wo.Positions(startPosition, endPosition)
		if err != nil {
			return fmt.Errorf("failed to compute labels: %w", err)
		}

		// sanity check with reference oracle
		reference, err := woReference.Position(endPosition)
		if err != nil {
			return fmt.Errorf("failed to compute referresultence label: %w", err)
		}
		if !bytes.Equal(res.Output[(batchSize-1)*postrs.LabelLength:], reference.Output) {
			return ErrReferenceLabelMismatch{
				Index:      endPosition,
				Commitment: init.commitmentAtxId,
				Expected:   reference.Output,
				Actual:     res.Output[(batchSize-1)*postrs.LabelLength:],
			}
		}
		// 这里返回res到存储机服务器，由存储机进行Nonce判断
		var result *pb.StreamResponse
		if res.Nonce != nil {
			result = &pb.StreamResponse{
				Output:           res.Output,
				Nonce:            *res.Nonce,
				StartPosition:    startPosition,
				NumLabelsWritten: fileOffset + currentPosition + uint64(batchSize),
			}
		} else {
			result = &pb.StreamResponse{
				Output:           res.Output,
				Nonce:            0,
				StartPosition:    startPosition,
				NumLabelsWritten: fileOffset + currentPosition + uint64(batchSize),
			}
		}

		if err := stream.Send(result); err != nil {
			return err
		}
	}
	stream.Context().Done()

	return nil
}

func WithIndex(index int64) OptionFunc {
	return func(opts *option) error {
		opts.index = index
		return nil
	}
}

func WithNumLabelsWritten(numLabelsWritten uint64) OptionFunc {
	return func(opts *option) error {
		opts.numLabelsWritten = numLabelsWritten
		return nil
	}
}
