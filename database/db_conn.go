package database

import (
	"time"

	"github.com/jinzhu/gorm"
	uuid "github.com/satori/go.uuid"
)

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

type ModelBase struct {
	UID       string    `gorm:"type:VARCHAR(36);primary_key;" json:"uid"`
	CreatedAt time.Time `gorm:"type:datetime" json:"created_at"`
	UpdatedAt time.Time `gorm:"type:datetime" json:"updated_at"`
}

func (mb *ModelBase) BeforeCreate(tx *gorm.DB) {
	mb.UID = uuid.NewV4().String()
}
