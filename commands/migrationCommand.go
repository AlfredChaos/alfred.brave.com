package commands

import (
	"flag"
	"os"

	"alfred.brave.com/conf"
	"github.com/pressly/goose"
	"github.com/urfave/cli"
)

var MigrationCommand = cli.Command{
	Name:    "migration",
	Aliases: []string{"goose"},
	Usage:   "database version migration tools",
	Action:  startAction,
}

var (
	flags = flag.NewFlagSet("goose", flag.ExitOnError)
	dir   = flags.String("dir", ".", "directory with migration files")
)

func migrationAction(ctx *cli.Context) error {
	config, err := conf.InitConfig(ctx)
	if err != nil {
		return err
	}

	flags.Parse(os.Args[1:])
	args := flags.Args()

	if len(args) < 3 {
		flags.Usage()
		return nil
	}

	arguments := []string{}
	if len(args) > 3 {
		arguments = append(arguments, args[3:]...)
	}

	if err := goose.Run(args[2], config.SqlDb(), *dir, arguments...); err != nil {
		log.Fatalf("goose %v: %v", args[2], err)
	}

	return nil
}
