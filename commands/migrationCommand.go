package commands

import (
	"github.com/urfave/cli"
)

var MigrationCommand = cli.Command{
	Name:        "migration",
	Aliases:     []string{"goose"},
	Usage:       "database version migration tools",
	Subcommands: migrationCommands,
}

var migrationCommands = []cli.Command{
	{
		Name:   "status",
		Usage:  "Dump the migration status for the current DB",
		Action: gooseStatusGet,
	},
	{
		Name:   "version",
		Usage:  "Print the current version of the database",
		Action: gooseVersionGet,
	},
	{
		Name:   "create",
		Usage:  "Creates new migration file with the current timestamp",
		Flags:  createFlags,
		Action: gooseCreateExecute,
	},
	{
		Name:   "up",
		Usage:  "Migrate the DB to the most recent version available or a specific VERSION",
		Flags:  versionFlags,
		Action: gooseMigrateExecute,
	},
	{
		Name:   "down",
		Usage:  "Roll back the version by 1 or a sspecific VERSION",
		Flags:  versionFlags,
		Action: gooseMigrateRollback,
	},
}

var createFlags = []cli.Flag{
	cli.StringFlag{
		Name:  "name",
		Usage: "generate a script",
	},
	cli.StringFlag{
		Name:     "type",
		Usage:    "specifies script type",
		Required: true,
	},
}

var versionFlags = []cli.Flag{
	cli.StringFlag{
		Name:     "version",
		Usage:    "specific VERSION",
		Required: false,
	},
}

func gooseCreateExecute(ctx *cli.Context) error {
	command := "create"
	arguments := []string{}
	return migrationAction(ctx, command, arguments)
}

func gooseStatusGet(ctx *cli.Context) error {
	command := "status"
	arguments := []string{}
	return migrationAction(ctx, command, arguments)
}

func gooseMigrateExecute(ctx *cli.Context) error {
	command := "up"
	arguments := []string{}
	return migrationAction(ctx, command, arguments)
}

func gooseMigrateRollback(ctx *cli.Context) error {
	command := "down"
	arguments := []string{}
	return migrationAction(ctx, command, arguments)
}

func gooseVersionGet(ctx *cli.Context) error {
	command := "version"
	arguments := []string{}
	return migrationAction(ctx, command, arguments)
}

func migrationAction(ctx *cli.Context, command string, arguments []string) error {
	return nil
}
