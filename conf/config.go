package conf

import (
	"sync"

	"alfred.brave.com/event"
	"github.com/jinzhu/gorm"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"
	"gorm.io/gorm"
)

var log = event.Log
var once sync.Once

type Config struct {
	once    sync.Once
	db      *gorm.DB
	options *Options
}

func initLogger() {
	once.Do(func() {
		log.SetFormatter(&logrus.TextFormatter{
			DisableColors: false,
			FullTimestamp: true,
		})
		log.SetLevel(logrus.DebugLevel)
	})
}

func NewConfig(ctx *cli.Context) *Config {
	initLogger()

	c := &Config{
		options: NewOptions(ctx),
	}

	return c
}

func (c *Config) GetHttpHost() string {
	if c.options == nil || (c.options != nil && c.options.HttpHost == "") {
		log.debug("http host not set")
		return ""
	}
	return c.options.HttpHost
}

func (c *Config) GetHttpPort() int {
	if c.options == nil || (c.options != nil && c.options.HttpPort == 0) {
		log.debug("http port not set")
		return 0
	}
	return c.options.HttpPort
}
