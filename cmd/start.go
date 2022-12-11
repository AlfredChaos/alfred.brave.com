package main

import (
	"fmt"

	"alfred.brave.com/conf"
	"github.com/rs/zerolog/log"
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
	cli.BoolFlag{
		Name:  "config, c",
		Usage: "show config",
	},
}

func startAction(ctx *cli.Context) error {
	config := conf.NewConfig(ctx)

	fmt.Printf("Name                  Value\n")
	fmt.Printf("http-host             %s\n", config.GetHttpHost())
	fmt.Printf("http-port             %d\n", config.GetHttpPort())

	if config.GetHttpPort() < 1 || config.GetHttpPort() > 65535 {
		log.Error("server port must be a number between 1 and 65535")
	}

	// initialize the database
	config.InitDb()

	return nil
}
