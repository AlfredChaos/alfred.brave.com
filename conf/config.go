package conf

import (
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"alfred.brave.com/common"
	"alfred.brave.com/env"
	"alfred.brave.com/event"
	"github.com/fsnotify/fsnotify"
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

func initDefaultConfig() {
	event.ConfigYaml.SetDefault("debug", true)
	event.ConfigYaml.SetDefault("version", "v1")
	event.ConfigYaml.SetDefault("log.level", "debug")
	event.ConfigYaml.SetDefault("log.output", "stdout")
	event.ConfigYaml.SetDefault("log.file_path", "/var/log/brave/brave.log")
	event.ConfigYaml.SetDefault("bind_address", "0.0.0.0")
	event.ConfigYaml.SetDefault("bind_port", 37001)
	event.ConfigYaml.SetDefault("database_driver", "mysql")
	event.ConfigYaml.SetDefault("mysql.server", "0.0.0.0")
	event.ConfigYaml.SetDefault("mysql.port", 3306)
	event.ConfigYaml.SetDefault("mysql.user", "root")
	event.ConfigYaml.SetDefault("mysql.password", "")
	event.ConfigYaml.SetDefault("mysql.database", "brave")

	_, ok := env.Environment["PROJECT_PATH"]
	if !ok {
		env.New()
	}
	confPath := filepath.Join(env.Environment["PROJECT_PATH"], "etc")
	event.ConfigYaml.AddConfigPath(confPath)
	event.ConfigYaml.SetConfigName("config")
	event.ConfigYaml.SetConfigType("yaml")
	if err := event.ConfigYaml.ReadInConfig(); err != nil {
		log.Errorf("Read brave config.yaml fail")
		panic(err)
	}

	event.ConfigYaml.WatchConfig()
	event.ConfigYaml.OnConfigChange(func(in fsnotify.Event) {
		log.Infof("Config file changed: %s", in.Name)
	})
}

func initLogger() {
	once.Do(func() {
		log.SetFormatter(&logrus.TextFormatter{
			DisableColors: false,
			FullTimestamp: true,
		})

		output := event.ConfigYaml.GetString("log.output")
		switch output {
		case common.LogOutputStdout:
			log.SetOutput(os.Stdout)
		case common.LogOutputStderr:
			log.SetOutput(os.Stderr)
		case common.LogOutputFile:
			logPath := event.ConfigYaml.GetString("log.file_path")
			logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0655)
			if err != nil {
				log.Errorf("Brave open log %s fail", logPath)
				panic(err)
			}
			writer := []io.Writer{logFile}
			fileWriter := io.MultiWriter(writer...)
			log.Infof("Brave Log redirect to %s", logPath)
			log.SetOutput(fileWriter)
		}

		level := event.ConfigYaml.GetString("log.level")
		switch level {
		case common.LogLevelDebug:
			log.SetLevel(logrus.DebugLevel)
		case common.LogLevelInfo:
			log.SetLevel(logrus.InfoLevel)
		case common.LogLevelWarn:
			log.SetLevel(logrus.WarnLevel)
		case common.LogLevelError:
			log.SetLevel(logrus.ErrorLevel)
		}
	})
}

func NewConfig(ctx *cli.Context) *Config {
	initDefaultConfig()
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
