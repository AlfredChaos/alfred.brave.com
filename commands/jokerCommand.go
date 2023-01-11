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
	"alfred.brave.com/joker"
	"github.com/urfave/cli"
)

var JokerCommand = cli.Command{
	Name:    "joker",
	Aliases: []string{"jk"},
	Usage:   "Starts the joker server",
	Action:  jokerAction,
}

func jokerAction(ctx *cli.Context) error {
	middlewares := registerJokerMiddlewares()
	config, err := conf.InitConfig(ctx, common.JokerName, middlewares)
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

	// Start web server
	go joker.Start(cctx, config)

	// Wait for signal to initiate server shutdown
	quit := make(chan os.Signal)
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

func registerJokerMiddlewares() []int {
	return []int{
		common.MiddlewareEtcd,
	}
}
