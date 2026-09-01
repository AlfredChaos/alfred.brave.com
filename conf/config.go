package conf

import (
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"alfred.brave.com/common"
	"alfred.brave.com/database"
	"alfred.brave.com/env"
	"alfred.brave.com/event"
	"github.com/fsnotify/fsnotify"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"
	clientv3 "go.etcd.io/etcd/client/v3"
)

var log = event.Log
var once sync.Once

type Config struct {
	pg         *database.Store
	options    *Options
	EtcdClient *clientv3.Client
}

func initDefaultConfig(name string) {
	event.ConfigYaml.SetDefault("debug", true)
	event.ConfigYaml.SetDefault("version", "v1")
	event.ConfigYaml.SetDefault("log.level", "debug")
	event.ConfigYaml.SetDefault("log.output", "stdout")
	event.ConfigYaml.SetDefault("log.file_path", "/var/log/brave/brave.log")
	event.ConfigYaml.SetDefault("bind_address", "0.0.0.0")
	event.ConfigYaml.SetDefault("bind_port", 37001)
	event.ConfigYaml.SetDefault("database_driver", "postgres")
	event.ConfigYaml.SetDefault("postgres.server", "127.0.0.1")
	event.ConfigYaml.SetDefault("postgres.port", 5432)
	event.ConfigYaml.SetDefault("postgres.user", "brave")
	event.ConfigYaml.SetDefault("postgres.password", "brave")
	event.ConfigYaml.SetDefault("postgres.database", "brave")
	event.ConfigYaml.SetDefault("auth.secret", "brave-dev-secret")
	event.ConfigYaml.SetDefault("kafka.brokers", "127.0.0.1:9092")
	event.ConfigYaml.SetDefault("etcd.dial_timeout", 5)
	event.ConfigYaml.SetDefault("etcd.endpoints", "0.0.0.0:2379,")

	if os.Getenv("PROJECT_PATH") == "" {
		env.New()
	}

	confPath := filepath.Join(os.Getenv("PROJECT_PATH"), "etc")
	event.ConfigYaml.AddConfigPath(confPath)
	event.ConfigYaml.SetConfigName(name)
	event.ConfigYaml.SetConfigType("yaml")
	if err := event.ConfigYaml.ReadInConfig(); err != nil {
		log.Errorf("Read brave config.yaml fail")
		panic(err)
	}

	log.Info("Enable Config Watching.")
	event.ConfigYaml.WatchConfig()
	event.ConfigYaml.OnConfigChange(func(in fsnotify.Event) {
		log.Infof("Config file changed: %s", in.Name)
	})
}

func initLogger() {
	once.Do(func() {
		customFormatter := new(logrus.TextFormatter)
		customFormatter.TimestampFormat = common.TimeFormat
		customFormatter.DisableColors = false
		customFormatter.FullTimestamp = true
		log.SetFormatter(customFormatter)

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

func InitConfig(ctx *cli.Context, service string, middlewares []int) (*Config, error) {
	c := newConfig(ctx, service)
	return c, c.init(middlewares)
}

func newConfig(ctx *cli.Context, service string) *Config {
	log.Info("Start init default config")
	initDefaultConfig(service)
	log.Info("Start init global logger")
	initLogger()

	c := &Config{
		options: NewOptions(ctx, service),
	}

	return c
}

func (c *Config) init(middlewares []int) error {
	start := time.Now()

	for _, mw := range middlewares {
		switch mw {
		case common.MiddlewareDatabase:
			log.Info("Prepare to connect database")
			if err := c.ConnectDb(); err != nil {
				return err
			}

		case common.MiddlewareEtcd:
			log.Info("Prepare to connect etcd")
			if err := c.ConnectEtcd(); err != nil {
				return err
			}
		}
	}

	log.Debugf("config: successfully initialized [%s]", time.Since(start))
	return nil
}

func (c *Config) GetHttpHost() string {
	if c.options == nil || (c.options != nil && c.options.HttpHost == "") {
		log.Debug("http host not set")
		return ""
	}
	return c.options.HttpHost
}

// GetGrpcPort 返回 gRPC 投递监听端口（joker 专用，默认 37012）。
func (c *Config) GetGrpcPort() int {
	if c.options == nil || c.options.GrpcPort == 0 {
		return 37012
	}
	return c.options.GrpcPort
}

func (c *Config) GetHttpPort() int {
	if c.options == nil || (c.options != nil && c.options.HttpPort == 0) {
		log.Debug("http port not set")
		return 0
	}
	return c.options.HttpPort
}

// KafkaBrokers 返回 Kafka broker 地址列表（逗号分隔配置）。
func (c *Config) KafkaBrokers() []string {
	if c.options == nil || len(c.options.KafkaBrokers) == 0 {
		return []string{"127.0.0.1:9092"}
	}
	return c.options.KafkaBrokers
}

// AuthSecret 返回网关鉴权签名密钥；未配置时用开发默认值（生产必须显式配置）。
func (c *Config) AuthSecret() string {
	if c.options == nil || c.options.AuthSecret == "" {
		return "brave-dev-secret"
	}
	return c.options.AuthSecret
}

// GetAdvertiseHost 返回本节点对外可路由的主机地址（用于注册到 etcd 与回传给客户端）。
// 未配置 advertise_host 时回退到监听地址 bind_address，保证本地裸跑行为不变。
func (c *Config) GetAdvertiseHost() string {
	if c.options == nil {
		return ""
	}
	if c.options.AdvertiseHost != "" {
		return c.options.AdvertiseHost
	}
	return c.GetHttpHost()
}

func (c *Config) Shutdown() {
	if c.pg != nil {
		if err := c.CloseDb(); err != nil {
			log.Errorf("could not close database connection: %s", err)
		}
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
