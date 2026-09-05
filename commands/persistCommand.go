package commands

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"alfred.brave.com/common"
	"alfred.brave.com/conf"
	"alfred.brave.com/internal/chat"
	ibrave "alfred.brave.com/internal/kafka"
	"alfred.brave.com/worker/persist"
	"github.com/urfave/cli"
)

var PersistCommand = cli.Command{
	Name:   "persist",
	Usage:  "Runs the persist worker (chat.msg -> tx insert -> chat.push)",
	Action: persistAction,
}

func persistAction(ctx *cli.Context) error {
	config, err := conf.InitConfig(ctx, common.PersistName, []int{common.MiddlewareDatabase})
	if err != nil {
		return err
	}

	cctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 本地/裸跑环境确保 topic 存在（幂等）；生产 compose 由 init 容器负责
	if err := ibrave.EnsureTopics(config.KafkaBrokers(), chat.Partitions); err != nil {
		log.Warnf("ensure topics failed (broker not up?): %v", err)
	}

	push := ibrave.NewProducer(config.KafkaBrokers(), chat.TopicPush)
	defer push.Close()

	worker := persist.New(config.Db(), push, config.KafkaBrokers(), "persist")

	workerErr := make(chan error, 1)
	go func() { workerErr <- worker.Run(cctx) }()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	select {
	case err := <-workerErr:
		if err == nil {
			cancel()
			config.Shutdown()
			return nil
		}
		// 致命退出必须让进程非零退出（at-least-once 依赖部署层重启重投）；
		// 只 cancel 会卡在 <-quit 变僵尸，restart 永不触发
		config.Shutdown()
		return fmt.Errorf("persist worker exited: %w", err)
	case <-quit:
		log.Info("persist: shutting down")
		cancel()
		config.Shutdown()
		return nil
	}
}
