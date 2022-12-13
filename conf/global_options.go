package conf

import (
	"alfred.brave.com/common"
	"alfred.brave.com/event"
	"github.com/urfave/cli"
)

type Options struct {
	Name             string `json:"name"`
	LogLevel         string `json:"LogLevel"`
	AdminPassword    string `json:"AdminPassword"`
	ConfigPath       string `json:"ConfigPath"`
	DatabaseServer   string `json:"DatabaseServer"`
	DatabaseName     string `json:"DatabaseName"`
	DatabaseUser     string `json:"DatabaseUser"`
	DatabasePassword string `json:"DatabasePassword"`
	HttpHost         string `json:"HttpHost"`
	HttpPort         int    `json:"HttpPort"`
	LogFilename      string `json:"LogFilename"`
	SiteUrl          string `json:"SiteUrl"`
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

	return c
}
