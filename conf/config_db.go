package conf

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strconv"

	"alfred.brave.com/database"
	"alfred.brave.com/internal/etcd"
	"alfred.brave.com/internal/mutex"
)

// PostgreSQL 驱动（T02 起替换 MySQL）。
const Postgres = "postgres"

// Db 返回 PG 数据访问层。未连接时返回 nil 并告警（沿用原 Get 行为，调用方自查）。
func (c *Config) Db() *database.Store {
	if c.pg == nil {
		log.Error("config: database not connected")
	}
	return c.pg
}

func (c *Config) InitDb() {
	// pgx 版无 gorm 的表选项注册步骤，保留空实现维持中间件调用序列
}

func (c *Config) CloseDb() error {
	if c.pg != nil {
		c.pg.Close()
		c.pg = nil
		log.Info("closed database connection.")
	}
	return nil
}

func (c *Config) ConnectEtcd() error {
	mutex.EtcdMutex.Lock()
	defer mutex.EtcdMutex.Unlock()

	c.EtcdClient = etcd.NewEtcdClient(c.options.EtcdEndpoints, c.options.EtcdDialTimeout)
	if c.EtcdClient == nil {
		return errors.New("Get etcd client nil")
	}
	etcd.RegisterEtcdConn(c.EtcdClient)
	return nil
}

// ConnectDb 建立 pgx 连接池（带重试），失败返回错误。
func (c *Config) ConnectDb() error {
	mutex.Db.Lock()
	defer mutex.Db.Unlock()

	dsn := c.DatabaseDsn()
	log.Infof("Get database driver: %s, server: %s", c.DatabaseDriver(), c.DatabaseConnAddress())

	store, err := database.NewStore(context.Background(), dsn, int32(c.DatabaseConns()))
	if err != nil || store == nil {
		return err
	}
	c.pg = store
	return nil
}

func (c *Config) DatabaseDriver() string {
	return Postgres
}

func (c *Config) DatabaseUser() string {
	if c.options.DatabaseUser == "" {
		return "brave"
	}
	return c.options.DatabaseUser
}

func (c *Config) DatabasePassword() string {
	return c.options.DatabasePassword
}

func (c *Config) DatabaseName() string {
	if c.options.DatabaseName == "" {
		return "brave"
	}
	return c.options.DatabaseName
}

func (c *Config) DatabasePort() int {
	defaultPort := 5432
	if c.options.DatabasePort < 1 || c.options.DatabasePort > 65535 {
		log.Errorf("Config Database port %d error: range 1-65535", c.options.DatabasePort)
		return defaultPort
	}
	return c.options.DatabasePort
}

func (c *Config) DatabasePortString() string {
	return strconv.Itoa(c.DatabasePort())
}

func (c *Config) DatabaseServer() string {
	if c.options.DatabaseServer == "" {
		return "127.0.0.1"
	}
	return c.options.DatabaseServer
}

func (c *Config) DatabaseConnAddress() string {
	return fmt.Sprintf("%s:%s", c.DatabaseServer(), c.DatabasePortString())
}

// DatabaseDsn PG 连接串。本地/容器内一律 sslmode=disable（练手项目无 TLS 终结）。
func (c *Config) DatabaseDsn() string {
	if c.options.DatabaseDsn != "" {
		return c.options.DatabaseDsn
	}
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable",
		c.DatabaseUser(), c.DatabasePassword(), c.DatabaseServer(), c.DatabasePort(), c.DatabaseName())
}

// DatabaseConns 连接池上限（对齐原 gorm 时代的推导公式）。
func (c *Config) DatabaseConns() int {
	limit := c.options.DatabaseConns
	if limit <= 0 {
		limit = (runtime.NumCPU() * 2) + 16
	}
	if limit > 1024 {
		limit = 1024
	}
	return limit
}

// DatabaseConnsIdle 空闲连接上限（推导保留；pgxpool 以 MaxConns 为准）。
func (c *Config) DatabaseConnsIdle() int {
	limit := c.options.DatabaseConnsIdle
	if limit <= 0 {
		limit = runtime.NumCPU() + 8
	}
	if limit > c.DatabaseConns() {
		limit = c.DatabaseConns()
	}
	return limit
}
