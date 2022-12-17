package database

import "github.com/jinzhu/gorm"

type Gorm interface {
	Db() *gorm.DB
}

var dbConn Gorm

func SetDbProvider(conn Gorm) {
	dbConn = conn
}

func DbClient() *gorm.DB {
	return dbConn.Db()
}
