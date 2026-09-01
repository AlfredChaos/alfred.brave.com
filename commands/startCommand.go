package commands

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"alfred.brave.com/common"
	"alfred.brave.com/conf"
	"alfred.brave.com/server"
	"github.com/urfave/cli"
)

var StartCommand = cli.Command{
	Name:    "start",
	Aliases: []string{"up"},
	Usage:   "Starts the web server",
	Flags:   startFlags,
	Action:  startAction,
}

var startFlags = []cli.Flag{
	cli.StringFlag{
		Name:  "config",
		Usage: "show config",
	},
}

func startAction(ctx *cli.Context) error {
	middlewares := registerBraveMiddlewares()
	config, err := conf.InitConfig(ctx, common.ProjectName, middlewares)
	if err != nil {
		return err
	}

	fmt.Printf("Name                  Value\n")
	fmt.Printf("http-host             %s\n", config.GetHttpHost())
	fmt.Printf("http-port             %d\n", config.GetHttpPort())

	if config.GetHttpPort() < 1 || config.GetHttpPort() > 65535 {
		log.Error("Server port must be a number between 1 and 65535")
	}

	// Pass this context down the chain
	cctx, cancel := context.WithCancel(context.Background())

	// initialize the database
	config.InitDb()

	// Start web server
	go server.Start(cctx, config)

	// Wait for signal to initiate server shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	<-quit

	// Stop all background activity
	// ...

	log.Info("shutting down...")
	cancel()

	// Finally, close the DB connection after a short grace period
	time.Sleep(2 * time.Second)
	config.Shutdown()

	return nil
}

func registerBraveMiddlewares() []int {
	return []int{
		common.MiddlewareMysql,
		common.MiddlewareEtcd,
	}
}
