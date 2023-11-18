package service

import (
	"context"

	"github.com/spacemeshos/post/config"
	"github.com/spacemeshos/post/initialization"
	"github.com/spacemeshos/post/proving"
	"github.com/spacemeshos/post/shared"
	"go.uber.org/zap"
)

type RemotePost struct {
	initialization *initialization.Initializer
	id             []byte
	log            *zap.Logger
}

type Args struct{}

type GenerateArgs struct {
	ch  shared.Challenge
	cfg config.Config
}

type GenerateReply struct {
}

func (p RemotePost) RemoteNumLabelsWritten(args *Args, reply uint64) error {
	reply = p.initialization.NumLabelsWritten()
	return nil
}

func (p RemotePost) RemoteReset() error {
	if err := p.initialization.Reset(); err != nil {
		return err
	}
	return nil
}

func (p RemotePost) RemoteGenerate(args *GenerateArgs, reply *GenerateReply) error {
	// 读取本地配置
	opt = proving.WithDataSource(args.cfg, p.id)
	proof, proofMetadata, err := proving.Generate(context.TODO(), args.ch, args.cfg, p.log)
}
