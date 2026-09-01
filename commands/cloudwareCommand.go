package commands

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"alfred.brave.com/cloudware"
	"alfred.brave.com/common"
	"alfred.brave.com/conf"
	"github.com/urfave/cli"
)

var CloudwareCommand = cli.Command{
	Name:    "cloudware",
	Aliases: []string{"cloud"},
	Usage:   "Starts the cloudware server",
	Action:  cloudwareAction,
}

func cloudwareAction(ctx *cli.Context) error {
	middlewares := registerCloudwareMiddlewares()
	config, err := conf.InitConfig(ctx, common.CloudwareName, middlewares)
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
	go cloudware.Start(cctx, config)

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

func registerCloudwareMiddlewares() []int {
	return []int{
		common.MiddlewareDatabase,
		common.MiddlewareEtcd,
	}
}
