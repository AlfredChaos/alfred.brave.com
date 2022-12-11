package main

import (
	"log"
	"os"

	"alfred.brave.com/common"

	"github.com/urfave/cli"
)

func main() {
	defer func() {
		if r := recover(); r != nil {
			os.Exit(1)
		}
	}()

	app := cli.NewApp()
	app.Name = common.ProjectName
	app.Usage = "make urfave project"
	app.Commands = Braves

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
