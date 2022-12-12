package main

import (
	"os"

	"alfred.brave.com/commands"
	"alfred.brave.com/common"
	"alfred.brave.com/event"

	"github.com/urfave/cli"
)

var log = event.Log

func main() {
	defer func() {
		if r := recover(); r != nil {
			os.Exit(1)
		}
	}()

	app := cli.NewApp()
	app.Name = common.ProjectName
	app.Usage = "make urfave project"
	app.Commands = commands.Braves

	if err := app.Run(os.Args); err != nil {
		log.Error(err)
	}
}
