package commands

import (
	"alfred.brave.com/event"
	"github.com/urfave/cli"
)

var log = event.Log

var Braves = []cli.Command{
	StartCommand,
	MigrationCommand,
}
