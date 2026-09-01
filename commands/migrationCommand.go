package commands

import (
	"context"
	"database/sql"
	"fmt"
	"os"

	"alfred.brave.com/common"
	"alfred.brave.com/conf"
	"alfred.brave.com/env"

	_ "github.com/jackc/pgx/v5/stdlib" // 注册 database/sql 的 pgx 驱动（goose 需要）
	"github.com/pressly/goose/v3"
	"github.com/urfave/cli"
)

var MigrationCommand = cli.Command{
	Name:        "migration",
	Aliases:     []string{"goose"},
	Usage:       "database version migration tools (postgres)",
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
		Usage:  "Roll back the version by 1 or to a specific VERSION",
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
	scriptName := ctx.String("name")
	if ctx.Bool("init") {
		scriptName = "init"
	}
	switch ctx.String("type") {
	case "sql", "go":
	default:
		log.Error("unsupport type")
		return nil
	}
	return migrationAction(ctx, "create", []string{scriptName, ctx.String("type")})
}

func gooseStatusGet(ctx *cli.Context) error {
	return migrationAction(ctx, "status", nil)
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
	return migrationAction(ctx, "version", nil)
}

// migrationAction 用 goose v3 执行迁移子命令（postgres 方言）。
// goose 需要 *sql.DB：经 pgx stdlib 驱动开独立短连接，不与业务连接池争用（迁移是低频运维操作）。
func migrationAction(ctx *cli.Context, command string, arguments []string) error {
	if os.Getenv("PROJECT_PATH") == "" {
		env.New()
	}
	migrationPath := fmt.Sprintf("%s/%s", common.ProjectPath(), "database/migration")

	config, err := conf.InitConfig(ctx, common.ProjectName, []int{common.MiddlewareDatabase})
	if err != nil {
		return err
	}

	sqlDb, err := sql.Open("pgx", config.DatabaseDsn())
	if err != nil {
		log.Errorf("open sql handle for goose failed: %v", err)
		return err
	}
	defer sqlDb.Close()

	goose.SetVerbose(true)
	if err := goose.SetDialect("postgres"); err != nil {
		log.Errorf("set goose dialect postgres error: %v", err)
		return err
	}
	if err := goose.RunContext(context.Background(), command, sqlDb, migrationPath, arguments...); err != nil {
		log.Errorf("migration occurs error: %v", err)
		return err
	}
	return nil
}
