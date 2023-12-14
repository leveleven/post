package rpc

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"strconv"

	"github.com/spacemeshos/post/initialization"
)

const edKeyFileName = "key.bin"

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
	idHex              string
	id                 []byte
	commitmentAtxIdHex string
	numUnits           numUnitsFlag
	DataDir            string

	ErrKeyFileExists = errors.New("key file already exists")
)

func parseFlags() {
	flag.StringVar(&DataDir, "datadir", DefaultDataDir, "filesystem datadir path")
	flag.StringVar(&idHex, "id", "", "miner's id (public key), in hex (will be auto-generated if not provided)")
	flag.StringVar(&commitmentAtxIdHex, "commitmentAtxId", "", "commitment atx id, in hex (required)")
	flag.Var(&numUnits, "numUnits", "number of units")
}

func nodeServer() {
	client, err := rpc.Dial("tcp", "localhost:1234")
	if err != nil {
		fmt.Println("Error connecting to server:", err)
		return
	}
	defer client.Close()

	if idHex == "" {
		pub, priv, err := ed25519.GenerateKey(nil)
		if err != nil {
			fmt.Errorf("failed to generate identity: %w", err)
		}
		id = pub
		log.Printf("cli: generated id %x\n", id)
		if err := saveKey(priv); err != nil {
			fmt.Errorf("save key failed: ", err)
		}
	}
	// 这里拆分numUnits 做一个kv数据库
	ctx, _ := context.WithCancel(context.Background())
	args := &PlotOption{
		IDHex:              idHex,
		CommitmentAtxIdHex: commitmentAtxIdHex,
		NumUnits:           numUnits.value,
		Index:              0,
		ctx:                ctx,
	}

	var reply initialization.InitializerSingle
	if err := client.Call("RemotePlotServer.plot", args, &reply); err != nil {
		fmt.Println("Error calling remote method:", err)
		return
	}

	var res = <-reply.result

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
