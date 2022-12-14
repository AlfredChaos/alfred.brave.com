package conf

import (
	"alfred.brave.com/common"
	"alfred.brave.com/event"
	"github.com/urfave/cli"
)

type Options struct {
	Name              string `json:"name"`
	LogLevel          string `json:"LogLevel"`
	AdminPassword     string `json:"AdminPassword"`
	ConfigPath        string `json:"ConfigPath"`
	DatabaseDriver    string `json:"DatabaseDriver"`
	DatabaseServer    string `json:"DatabaseServer"`
	DatabasePort      int    `json:"DatabasePort"`
	DatabaseName      string `json:"DatabaseName"`
	DatabaseUser      string `json:"DatabaseUser"`
	DatabaseDsn       string `json:"DatabaseDsn"`
	DatabasePassword  string `json:"DatabasePassword"`
	DatabaseConns     int    `json:"DatabaseConns"`
	DatabaseConnsIdle int    `json:"DatabaseConnsIdle"`
	HttpHost          string `json:"HttpHost"`
	HttpPort          int    `json:"HttpPort"`
	LogFilename       string `json:"LogFilename"`
	SiteUrl           string `json:"SiteUrl"`
}

func NewOptions(ctx *cli.Context) *Options {
	c := &Options{}

	if ctx == nil {
		return c
	}

	c.Name = common.ProjectName
	c.LogLevel = event.ConfigYaml.GetString("log.level")
	c.LogFilename = event.ConfigYaml.GetString("log.file_path")
	c.HttpHost = event.ConfigYaml.GetString("bind_address")
	c.HttpPort = event.ConfigYaml.GetInt("bind_port")
	c.DatabaseDriver = event.ConfigYaml.GetString("database_driver")
	c.DatabaseName = event.ConfigYaml.GetString("mysql.database")
	c.DatabaseServer = event.ConfigYaml.GetString("mysql.server")
	c.DatabasePort = event.ConfigYaml.GetInt("mysql.port")
	c.DatabaseUser = event.ConfigYaml.GetString("mysql.user")
	c.DatabasePassword = event.ConfigYaml.GetString("mysql.password")

	return c
}
