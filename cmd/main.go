package main

import (
	"fmt"
	"log"
	"os"

	"alfred.brave.com/common"

	"github.com/urfave/cli"
)

func main() {
	app := cli.NewApp()
	app.Name = common.ProjectName
	app.Usage = "make urfave project"
	app.Action = func(c *cli.Context) error {
		fmt.Println("boom! I say !")
		return nil
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
