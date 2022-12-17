package conf

import (
	"database/sql"
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"time"

	"alfred.brave.com/common/utils"
	"alfred.brave.com/database"
	"alfred.brave.com/internal/mutex"
	"github.com/jinzhu/gorm"
)

const (
	MySQL   = "mysql"
	MariaDB = "mariadb"
)

func (c *Config) Db() *gorm.DB {
	if c.db == nil {
		log.Error("config: database not connected")
	}

	return c.db
}

func (c *Config) SqlDb() *sql.DB {
	if c.db == nil {
		log.Warn("config: database not connected.")
		c.init()
	}
	return c.db.DB()
}

// SetDbOptions sets the database collation to unicode if supported.
func (c *Config) SetDbOptions() {
	switch c.DatabaseDriver() {
	case MySQL, MariaDB:
		c.Db().Set("gorm:table_options", "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci")

	default:
		log.Error("config: unsupported database driver")
	}
}

func (c *Config) RegisterDb() {
	c.SetDbOptions()
	database.SetDbProvider(c)
}

func (c *Config) MigrateDb() {

}

func (c *Config) InitDb() {
	c.RegisterDb()
	c.MigrateDb()
}

func (c *Config) CloseDb() error {
	if c.db != nil {
		if err := c.db.Close(); err == nil {
			c.db = nil
		} else {
			return err
		}
	}
	return nil
}

func (c *Config) ConnectDb() error {
	mutex.Db.Lock()
	defer mutex.Db.Unlock()

	dbDriver := c.DatabaseDriver()
	dbDsn := c.DatabaseDsn()

	if dbDriver == "" {
		return errors.New("config: database driver not specified")
	}
	if dbDsn == "" {
		return errors.New("config: database dsn not specified")
	}

	db, err := gorm.Open(dbDriver, dbDsn)
	if err != nil || db == nil {
		// retry
		for i := 1; i <= 12; i++ {
			db, err = gorm.Open(dbDriver, dbDsn)
			if db != nil && err == nil {
				break
			}
			time.Sleep(5 * time.Second)
		}

		if err != nil || db == nil {
			return err
		}
	}

	// Configure database logging
	db.LogMode(true)
	db.SetLogger(log)

	// Set database connection parameters.
	db.DB().SetMaxOpenConns(c.DatabaseConns())
	db.DB().SetMaxIdleConns(c.DatabaseConnsIdle())
	db.DB().SetConnMaxLifetime(time.Hour)

	// Check database server version.
	if err = c.checkDb(db); err != nil {
		log.Error("connect database error")
		return err
	}
	db.New()

	// Ok.
	c.db = db

	return nil
}

func (c *Config) DatabaseDriver() string {
	switch strings.ToLower(c.options.DatabaseDriver) {
	case MySQL, MariaDB:
		c.options.DatabaseDriver = MySQL

	default:
		log.Warnf("config: unsupported database driver %s, using mysql", c.options.DatabaseDriver)
		c.options.DatabaseDriver = MySQL
	}

	return c.options.DatabaseDriver
}

func (c *Config) DatabaseUser() string {
	if c.options.DatabaseUser == "" {
		return "root"
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
	defaultPort := 3306

	if server := c.DatabaseServer(); server == "" {
		return defaultPort
	}
	port := c.options.DatabasePort
	if port < 1 || port > 65535 {
		log.Errorf("Config Database port %d error: range 1-65535", port)
		return defaultPort
	}
	return port
}

func (c *Config) DatabasePortString() string {
	return strconv.Itoa(c.DatabasePort())
}

func (c *Config) DatabaseServer() string {
	if c.options.DatabaseServer == "" {
		return "0.0.0.0"
	}
	return c.options.DatabaseServer
}

func (c *Config) DatabaseDsn() string {
	if c.options.DatabaseDsn == "" {
		switch c.DatabaseDriver() {
		case MySQL, MariaDB:
			address := c.DatabaseServer()
			// Connect via TCP or Unix Domain Socket?
			if strings.HasPrefix(address, "/") {
				log.Debugf("mariadb: connecting via Unix domain socket")
				address = fmt.Sprintf("unix(%s)", address)
			} else {
				address = fmt.Sprintf("tcp(%s)", address)
			}
			return fmt.Sprintf(
				"%s:%s@%s/%s?charset=utf8mb4,utf8&collation=utf8mb4_unicode_ci&parseTime=true",
				c.DatabaseUser(),
				c.DatabasePassword(),
				address,
				c.DatabaseName(),
			)

		default:
			log.Errorf("config: empty database dsn")
			return ""
		}
	}
	return c.options.DatabaseDsn
}

// DatabaseConns returns the maximum number of open connections to the database.
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

// DatabaseConnsIdle returns the maximum number of idle connections to the database (equal or less than open).
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

// connectDb checks the database server version.
func (c *Config) checkDb(db *gorm.DB) error {
	switch c.DatabaseDriver() {
	case MySQL:
		type Res struct {
			Value string `gorm:"column:Value;"`
		}
		var res Res
		if err := db.Raw("SHOW VARIABLES LIKE 'innodb_version'").Scan(&res).Error; err != nil {
			return nil
		} else if v := strings.Split(res.Value, "."); len(v) < 3 {
			log.Warnf("config: unknown database server version")
		} else if major := utils.UInt(v[0]); major < 10 {
			return fmt.Errorf("config: MySQL %s is not supported", res.Value)
		} else if sub := utils.UInt(v[1]); sub < 5 || sub == 5 && utils.UInt(v[2]) < 12 {
			return fmt.Errorf("config: MySQL %s is not supported", res.Value)
		}
	}

	return nil
}
