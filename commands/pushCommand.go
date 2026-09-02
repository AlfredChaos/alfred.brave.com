package commands

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"alfred.brave.com/common"
	"alfred.brave.com/conf"
	"alfred.brave.com/worker/push"
	"github.com/urfave/cli"
)

var PushCommand = cli.Command{
	Name:   "push",
	Usage:  "Runs the offline notification worker (chat.notify -> mock vendor channel)",
	Action: pushAction,
}

func pushAction(ctx *cli.Context) error {
	config, err := conf.InitConfig(ctx, common.PushName, []int{common.MiddlewareDatabase})
	if err != nil {
		return err
	}

	cctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 演示级：厂商通道为 mock（日志输出）；接真通道 = 实现 VendorChannel 新实现
	worker := push.New(push.LogVendor{}, config.KafkaBrokers(), "push")
	go func() {
		if err := worker.Run(cctx); err != nil {
			log.Errorf("push worker exited: %v", err)
			cancel()
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Info("push: shutting down")
	cancel()
	config.Shutdown()
	return nil
}
