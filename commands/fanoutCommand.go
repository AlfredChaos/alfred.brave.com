package commands

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"alfred.brave.com/common"
	"alfred.brave.com/conf"
	"alfred.brave.com/worker/fanout"
	"github.com/urfave/cli"
)

var FanoutCommand = cli.Command{
	Name:   "fanout",
	Usage:  "Runs the feed fanout worker (feed.fanout -> batch inbox writes)",
	Action: fanoutAction,
}

func fanoutAction(ctx *cli.Context) error {
	config, err := conf.InitConfig(ctx, common.FanoutName, []int{common.MiddlewareDatabase})
	if err != nil {
		return err
	}

	cctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	worker := fanout.New(config.Db(), config.KafkaBrokers(), "fanout")
	workerErr := make(chan error, 1)
	go func() { workerErr <- worker.Run(cctx) }()
	// 计数聚合定时任务与 fanout 消费同进程（feed 域后台 worker 的两个职责）
	go worker.RunCounters(cctx)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	select {
	case err := <-workerErr:
		if err == nil {
			cancel()
			config.Shutdown()
			return nil
		}
		// 致命退出必须让进程非零退出，由部署层重启拉起；只 cancel 会卡在
		// <-quit 变僵尸，restart 永不触发
		config.Shutdown()
		return fmt.Errorf("fanout worker exited: %w", err)
	case <-quit:
		log.Info("fanout: shutting down")
		cancel()
		config.Shutdown()
		return nil
	}
}
