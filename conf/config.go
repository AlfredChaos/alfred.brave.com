package conf

import (
	"fmt"
	"net/url"
	"strings"
	"sync"

	"alfred.brave.com/event"
	"github.com/jinzhu/gorm"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"
)

var log = event.Log
var once sync.Once

type Config struct {
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
		log.Debug("http host not set")
		return ""
	}
	return c.options.HttpHost
}

func (c *Config) GetHttpPort() int {
	if c.options == nil || (c.options != nil && c.options.HttpPort == 0) {
		log.Debug("http port not set")
		return 0
	}
	return c.options.HttpPort
}

func (c *Config) Shutdown() {

	if err := c.CloseDb(); err != nil {
		log.Errorf("could not close database connection: %s", err)
	} else {
		log.Info("closed database connection.")
	}
}

func (c *Config) SiteUrl() string {
	if c.options.SiteUrl == "" {
		siteUrl := fmt.Sprintf("http://%s:%d/", c.options.HttpHost, c.options.HttpPort)
		return siteUrl
	}
	return strings.TrimRight(c.options.SiteUrl, "/") + "/"
}

func (c *Config) BaseUri(base string) string {
	if c.SiteUrl() == "" {
		return base
	}

	u, err := url.Parse(c.SiteUrl())
	if err != nil {
		return base
	}

	return strings.TrimRight(u.EscapedPath(), "/") + base
}
