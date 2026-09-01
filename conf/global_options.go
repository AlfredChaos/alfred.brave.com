package conf

import (
	"strings"

	"alfred.brave.com/event"
	"github.com/urfave/cli"
)

type Options struct {
	Name              string   `json:"name"`
	LogLevel          string   `json:"LogLevel"`
	AdminPassword     string   `json:"AdminPassword"`
	ConfigPath        string   `json:"ConfigPath"`
	DatabaseDriver    string   `json:"DatabaseDriver"`
	DatabaseServer    string   `json:"DatabaseServer"`
	DatabasePort      int      `json:"DatabasePort"`
	DatabaseName      string   `json:"DatabaseName"`
	DatabaseUser      string   `json:"DatabaseUser"`
	DatabaseDsn       string   `json:"DatabaseDsn"`
	DatabasePassword  string   `json:"DatabasePassword"`
	DatabaseConns     int      `json:"DatabaseConns"`
	DatabaseConnsIdle int      `json:"DatabaseConnsIdle"`
	EtcdEndpoints     []string `json:"etcdEndpoints"`
	EtcdDialTimeout   int64    `json:"EtcdDialTimeout"`
	AuthSecret        string   `json:"AuthSecret"`
	HttpHost          string   `json:"HttpHost"`
	HttpPort          int      `json:"HttpPort"`
	GrpcPort          int      `json:"GrpcPort"`
	AdvertiseHost     string   `json:"AdvertiseHost"`
	LogFilename       string   `json:"LogFilename"`
	SiteUrl           string   `json:"SiteUrl"`
}

func NewOptions(ctx *cli.Context, service string) *Options {
	c := &Options{}

	if ctx == nil {
		return c
	}

	c.Name = service
	c.LogLevel = event.ConfigYaml.GetString("log.level")
	c.LogFilename = event.ConfigYaml.GetString("log.file_path")
	c.HttpHost = event.ConfigYaml.GetString("bind_address")
	c.HttpPort = event.ConfigYaml.GetInt("bind_port")
	c.GrpcPort = event.ConfigYaml.GetInt("grpc_port")
	c.AdvertiseHost = event.ConfigYaml.GetString("advertise_host")
	c.AuthSecret = event.ConfigYaml.GetString("auth.secret")
	c.DatabaseDriver = event.ConfigYaml.GetString("database_driver")
	c.DatabaseName = event.ConfigYaml.GetString("postgres.database")
	c.DatabaseServer = event.ConfigYaml.GetString("postgres.server")
	c.DatabasePort = event.ConfigYaml.GetInt("postgres.port")
	c.DatabaseUser = event.ConfigYaml.GetString("postgres.user")
	c.DatabasePassword = event.ConfigYaml.GetString("postgres.password")
	c.EtcdDialTimeout = event.ConfigYaml.GetInt64("etcd.dial_timeout")
	c.EtcdEndpoints = strings.Split(event.ConfigYaml.GetString("etcd.endpoints"), ",")

	return c
}
