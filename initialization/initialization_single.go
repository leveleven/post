package initialization

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/spacemeshos/post/config"
	"github.com/spacemeshos/post/internal/postrs"
	"github.com/spacemeshos/post/oracle"
	"github.com/spacemeshos/post/shared"
	"go.uber.org/zap"
)

// Initializer is responsible for initializing a new PoST commitment.
type InitializerSingle struct {
	nodeId          []byte
	commitmentAtxId []byte

	commitment []byte

	cfg  Config
	opts InitOpts

	// these values are atomics so they can be read from multiple other goroutines safely
	// write is protected by mtx
	nonceValue       atomic.Pointer[[]byte]
	nonce            atomic.Pointer[uint64]
	lastPosition     atomic.Pointer[uint64]
	numLabelsWritten atomic.Uint64

	diskState *DiskState
	mtx       sync.RWMutex // TODO(mafa): instead of a RWMutex we should lock with a lock file to prevent other processes from initializing/modifying the data

	logger            *Logger
	referenceOracle   *oracle.WorkOracle
	powDifficultyFunc func(uint64) []byte

	result chan oracle.WorkOracleResult
}

func NewSingleInitializer(opts ...OptionFunc) (*InitializerSingle, error) {
	options := &option{
		logger: zap.NewNop(),

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
		commitment:        options.commitment,
		diskState:         NewDiskState(options.initOpts.DataDir, uint(config.BitsPerLabel)),
		logger:            options.logger,
		powDifficultyFunc: options.powDifficultyFunc,
		referenceOracle:   options.referenceOracle,

		result: make(chan oracle.WorkOracleResult),
	}

	return init, nil
}

func (init *InitializerSingle) SingleInitialize(ctx context.Context) error {
	if !init.mtx.TryLock() {
		return ErrAlreadyInitializing
	}
	defer init.mtx.Unlock()

	layout, err := deriveFilesLayout(init.cfg, init.opts)
	if err != nil {
		return err
	}

	index := init.opts.FromFileIdx

	init.logger.Info("initialization started",
		zap.String("datadir", init.opts.DataDir),
		zap.Uint32("numUnits", init.opts.NumUnits),
		zap.Uint64("maxFileSize", init.opts.MaxFileSize),
		zap.Uint64("labelsPerUnit", init.cfg.LabelsPerUnit),
	)

	init.logger.Info("initialization file layout",
		zap.Uint64("labelsPerFile", layout.FileNumLabels),
		zap.Uint64("labelsLastFile", layout.LastFileNumLabels),
		zap.Int("FileIndex", index),
	)

	numLabels := uint64(init.opts.NumUnits) * init.cfg.LabelsPerUnit
	difficulty := init.powDifficultyFunc(numLabels)
	batchSize := init.opts.ComputeBatchSize

	wo, err := oracle.New(
		oracle.WithProviderID(init.opts.ProviderID),
		oracle.WithCommitment(init.commitment),
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
			oracle.WithCommitment(init.commitment),
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
	if err := init.initSingleFile(ctx, wo, woReference, index, batchSize, fileOffset, fileNumLabels, difficulty); err != nil {
		return err
	}

	return nil
}

func (init *InitializerSingle) initSingleFile(ctx context.Context, wo, woReference *oracle.WorkOracle, fileIndex int, batchSize, fileOffset, fileNumLabels uint64, difficulty []byte) error {
	numLabelsWritten := uint64(0)

	fields := []zap.Field{
		zap.Int("fileIndex", fileIndex),
		zap.Uint64("currentNumLabels", numLabelsWritten),
		zap.Uint64("targetNumLabels", fileNumLabels),
		zap.Uint64("startPosition", fileOffset),
	}

	init.logger.Info("initialization: starting to write file", fields...)

	for currentPosition := numLabelsWritten; currentPosition < fileNumLabels; currentPosition += batchSize {
		// The last batch might need to be smaller.
		remaining := fileNumLabels - currentPosition
		if remaining < batchSize {
			batchSize = remaining
		}

		init.logger.Debug("initialization: status",
			zap.Int("fileIndex", fileIndex),
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
				Commitment: init.commitment,
				Expected:   reference.Output,
				Actual:     res.Output[(batchSize-1)*postrs.LabelLength:],
			}
		}
		// 这里返回res到存储机服务器，由存储机进行Nonce判断
		init.result <- res
	}

	<-ctx.Done()
	return nil
}

func WithIndex(index int) OptionFunc {
	return func(opts *option) error {
		opts.index = index
		return nil
	}
}
