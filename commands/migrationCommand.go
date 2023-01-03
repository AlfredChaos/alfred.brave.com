package commands

import (
	"fmt"
	"os"

	"alfred.brave.com/common"
	"alfred.brave.com/conf"
	"alfred.brave.com/env"
	"github.com/pressly/goose"
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
	cli.BoolFlag{
		Name:  "init",
		Usage: "init origin script",
	},
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

	scriptName := ctx.String("name")
	if ctx.Bool("init") {
		scriptName = "init"
	}
	arguments = append(arguments, scriptName)
	switch ctx.String("type") {
	case "sql", "go":
		arguments = append(arguments, ctx.String("type"))
	default:
		log.Error("unsupport type")
		return nil
	}

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

	if ctx.String("version") != "" {
		command = "up-to"
		arguments = append(arguments, ctx.String("version"))
	}

	return migrationAction(ctx, command, arguments)
}

func gooseMigrateRollback(ctx *cli.Context) error {
	command := "down"
	arguments := []string{}

	if ctx.String("version") != "" {
		command = "down-to"
		arguments = append(arguments, ctx.String("version"))
	}

	return migrationAction(ctx, command, arguments)
}

func gooseVersionGet(ctx *cli.Context) error {
	command := "version"
	arguments := []string{}
	return migrationAction(ctx, command, arguments)
}

func migrationAction(ctx *cli.Context, command string, arguments []string) error {
	// Get migration files path
	if os.Getenv("PROJECT_PATH") == "" {
		env.New()
	}
	projectPath := os.Getenv("PROJECT_PATH")
	migrationPath := fmt.Sprintf("%s/%s", projectPath, "database/migration")

	// get database connection
	config, err := conf.InitConfig(ctx, common.ProjectName)
	if err != nil {
		return err
	}
	sqlDb := config.Db().DB()

	goose.SetVerbose(true)
	if err := goose.SetDialect(config.DatabaseDriver()); err != nil {
		log.Errorf("set goose dialect %s error", config.DatabaseDriver())
		return err
	}
	if err := goose.Run(command, sqlDb, migrationPath, arguments...); err != nil {
		log.Errorf("migration occurs error: %v", err)
		return err
	}

	return nil
}
