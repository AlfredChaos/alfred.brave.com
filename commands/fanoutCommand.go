package commands

import (
	"context"
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
	go func() {
		if err := worker.Run(cctx); err != nil {
			log.Errorf("fanout worker exited: %v", err)
			cancel()
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Info("fanout: shutting down")
	cancel()
	config.Shutdown()
	return nil
}
