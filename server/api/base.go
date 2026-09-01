package api

import (
	"alfred.brave.com/database"
	"alfred.brave.com/event"
)

var log = event.Log

var Services = make([]string, 0)

// Server 网关 API 的依赖容器：数据访问器经构造注入（替代原全局 gorm provider）。
type Server struct {
	users   *database.UserStore
	friends *database.FriendStore
}

func NewServer(store *database.Store) *Server {
	return &Server{
		users:   database.NewUserStore(store),
		friends: database.NewFriendStore(store),
	}
}
