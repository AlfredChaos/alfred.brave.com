package commands

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"alfred.brave.com/common"
	"alfred.brave.com/conf"
	"alfred.brave.com/feed"
	"github.com/urfave/cli"
)

var FeedCommand = cli.Command{
	Name:   "feed",
	Usage:  "Runs the feed api server (:37003)",
	Action: feedAction,
}

func feedAction(ctx *cli.Context) error {
	middlewares := []int{common.MiddlewareDatabase, common.MiddlewareEtcd}
	config, err := conf.InitConfig(ctx, common.FeedName, middlewares)
	if err != nil {
		return err
	}

	cctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go feed.Start(cctx, config)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Info("feed: shutting down")
	cancel()
	config.Shutdown()
	return nil
}
