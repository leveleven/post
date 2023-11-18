package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"os/signal"
	"path/filepath"

	"github.com/davecgh/go-spew/spew"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/spacemeshos/post/config"
	"github.com/spacemeshos/post/initialization"
	"github.com/spacemeshos/post/internal/postrs"
	"github.com/spacemeshos/post/proving"
	"github.com/spacemeshos/post/shared"
	"github.com/spacemeshos/post/verifying"
)

const edKeyFileName = "key.bin"

var (
	cfg                = config.MainnetConfig()
	opts               = config.MainnetInitOpts()
	printProviders     bool
	printNumFiles      bool
	printConfig        bool
	genkey             bool
	genProof           bool
	idHex              string
	id                 []byte
	commitmentAtxIdHex string
	commitmentAtxId    []byte
	reset              bool

	logLevel zapcore.Level

	ErrKeyFileExists = errors.New("key file already exists")
)

func parseFlags() {
	flag.TextVar(&logLevel, "logLevel", zapcore.InfoLevel, "log level (debug, info, warn, error, dpanic, panic, fatal)")

	flag.BoolVar(&printProviders, "printProviders", false, "print the list of compute providers")
	flag.BoolVar(&printNumFiles, "printNumFiles", false, "print the total number of files that would be initialized")
	flag.BoolVar(&printConfig, "printConfig", false, "print the used config and options")
	flag.BoolVar(&genkey, "genkey", false, "generate miner's key id")
	flag.BoolVar(&genProof, "genproof", false, "generate proof as a sanity test, after initialization")
	flag.StringVar(&opts.DataDir, "datadir", opts.DataDir, "filesystem datadir path")
	flag.Uint64Var(&opts.MaxFileSize, "maxFileSize", opts.MaxFileSize, "max file size")
	flag.IntVar(&opts.ProviderID, "provider", opts.ProviderID, "compute provider id (required)")
	flag.Uint64Var(&cfg.LabelsPerUnit, "labelsPerUnit", cfg.LabelsPerUnit, "the number of labels per unit")
	flag.BoolVar(&reset, "reset", false, "whether to reset the datadir before starting")
	flag.StringVar(&idHex, "id", "", "miner's id (public key), in hex (will be auto-generated if not provided)")
	flag.StringVar(&commitmentAtxIdHex, "commitmentAtxId", "", "commitment atx id, in hex (required)")
	flag.BoolVar(&opts.DisableComputeNonce, "disableComputeNonce", opts.DisableComputeNonce, "compute nonce switch")
	flag.BoolVar(&opts.AutoGenProof, "autoGenProof", opts.AutoGenProof, "auto generate proof")
	numUnits := flag.Uint64("numUnits", uint64(opts.NumUnits), "number of units")

	flag.IntVar(&opts.FromFileIdx, "fromFile", 0, "index of the first file to init (inclusive)")
	var to int
	flag.IntVar(&to, "toFile", math.MaxInt, "index of the last file to init (inclusive). Will init to the end of declared space if not provided.")
	flag.Parse()

	// A workaround to simulate an optional value w/o a default ¯\_(ツ)_/¯
	// The default will be known later, after parsing the flags.
	if to != math.MaxInt {
		opts.ToFileIdx = &to
	}
	opts.NumUnits = uint32(*numUnits) // workaround the missing type support for uint32
}

func processFlags() error {
	if opts.ProviderID < 0 {
		return errors.New("-provider flag is required")
	}

	if commitmentAtxIdHex == "" {
		return errors.New("-commitmentAtxId flag is required")
	}
	var err error
	commitmentAtxId, err = hex.DecodeString(commitmentAtxIdHex)
	if err != nil {
		return fmt.Errorf("invalid commitmentAtxId: %w", err)
	}

	if (opts.FromFileIdx != 0 || opts.ToFileIdx != nil) && idHex == "" {
		return errors.New("-id flag is required when using -fromFile or -toFile")
	}

	if idHex == "" {
		pub, priv, err := ed25519.GenerateKey(nil)
		if err != nil {
			return fmt.Errorf("failed to generate identity: %w", err)
		}
		id = pub
		log.Printf("cli: generated id %x\n", id)
		return saveKey(priv)
	}
	id, err = hex.DecodeString(idHex)
	if err != nil {
		return fmt.Errorf("invalid id: %w", err)
	}
	return nil
}

func main() {
	parseFlags()

	if printProviders {
		providers, err := postrs.OpenCLProviders()
		if err != nil {
			log.Fatalln("failed to get OpenCL providers", err)
		}
		// spew.Dump(providers)
		// fmt.Println(providers)
		j, _ := json.Marshal(providers)
		fmt.Println(string(j))
		return
	}

	if printNumFiles {
		totalFiles := opts.TotalFiles(cfg.LabelsPerUnit)
		fmt.Println(totalFiles)
		return
	}

	if printConfig {
		spew.Dump(cfg)
		spew.Dump(opts)
		return
	}

	if genkey {
		filename := filepath.Join(opts.DataDir, edKeyFileName)
		if _, err := os.Stat(filename); err == nil {
			f, err := os.ReadFile(filename)
			if err != nil {
				log.Fatalln("failed to open key file:", err)
				return
			}
			// defer f.Close()
			a, _ := hex.DecodeString(string(f))
			reader := bytes.NewBuffer(a)
			pub, _, err := ed25519.GenerateKey(reader)
			fmt.Printf("%x\n", pub)
			return
		}
		pub, priv, err := ed25519.GenerateKey(nil)
		if err != nil {
			log.Fatalln("failed to generate identity:", err)
			return
		}
		id = pub
		err_sk := saveKey(priv)
		if err_sk != nil {
			log.Fatalln("failed to save private key:", err_sk)
			return
		}
		fmt.Printf("%x\n", id)
		return
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

	zapLog, err := zapCfg.Build()
	if err != nil {
		log.Fatalln("failed to initialize zap logger:", err)
	}

	err = processFlags()
	switch {
	case errors.Is(err, ErrKeyFileExists):
		log.Fatalln("cli: key file already exists. This appears to be a mistake. If you're trying to initialize a new identity delete key.bin and try again otherwise specify identity with `-id` flag")
	case err != nil:
		log.Fatalln("failed to process flags", err)
	}

	init, err := initialization.NewInitializer(
		initialization.WithConfig(cfg),
		initialization.WithInitOpts(opts),
		initialization.WithNodeId(id),
		initialization.WithCommitmentAtxId(commitmentAtxId),
		initialization.WithLogger(zapLog),
	)
	if err != nil {
		log.Panic(err.Error())
	}

	if reset {
		if err := init.Reset(); err != nil {
			log.Fatalln("reset error", err)
		}
		log.Println("cli: reset completed")
		return
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	// 这里开始生成
	err = init.Initialize(ctx)
	switch {
	case errors.Is(err, shared.ErrInitCompleted):
		log.Panic(err.Error())
		return
	case errors.Is(err, context.Canceled):
		log.Println("cli: initialization interrupted")
		return
	case err != nil:
		log.Println("cli: initialization error", err)
		return
	}

	log.Println("cli: initialization completed")

	if genProof {
		log.Println("cli: generating proof as a sanity test")

		proof, proofMetadata, err := proving.Generate(ctx, shared.ZeroChallenge, cfg, zapLog, proving.WithDataSource(cfg, id, commitmentAtxId, opts.DataDir))
		if err != nil {
			log.Fatalln("proof generation error", err)
		}
		verifier, err := verifying.NewProofVerifier()
		if err != nil {
			log.Fatalln("failed to create verifier", err)
		}
		defer verifier.Close()
		if err := verifier.Verify(proof, proofMetadata, cfg, zapLog); err != nil {
			log.Fatalln("failed to verify test proof", err)
		}

		log.Println("cli: proof is valid")
	}
}

func saveKey(key ed25519.PrivateKey) error {
	if err := os.MkdirAll(opts.DataDir, 0o700); err != nil && !os.IsExist(err) {
		return fmt.Errorf("mkdir error: %w", err)
	}

	filename := filepath.Join(opts.DataDir, edKeyFileName)
	if _, err := os.Stat(filename); err == nil {
		return ErrKeyFileExists
	}

	if err := os.WriteFile(filename, []byte(hex.EncodeToString(key)), 0o600); err != nil {
		return fmt.Errorf("key write to disk error: %w", err)
	}
	return nil
}
