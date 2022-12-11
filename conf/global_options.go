package conf

import (
	"alfred.brave.com/common"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"
)

type Options struct {
	Name             string `json:"name"`
	LogLevel         string `json:"LogLevel""`
	AdminPassword    string `json:"AdminPassword"`
	ConfigPath       string `json:"ConfigPath"`
	DatabaseServer   string `json:"DatabaseServer"`
	DatabaseName     string `json:"DatabaseName"`
	DatabaseUser     string `json:"DatabaseUser"`
	DatabasePassword string `json:"DatabasePassword"`
	HttpHost         string `json:"HttpHost"`
	HttpPort         int    `json:"HttpPort"`
	LogFilename      string `json:"LogFilename"`
}

func NewOptions(ctx *cli.Context) *Options {
	c := &Options{}

	if ctx == nil {
		return c
	}

	c.Name = common.ProjectName
	c.LogLevel = common.LogLevelDebug
	c.LogFilename = "brave.log"
	c.HttpHost = "127.0.0.1"
	c.HttpPort = 8080

	return c
}
