package api

import (
	"time"

	"alfred.brave.com/database"
	"alfred.brave.com/event"
	"alfred.brave.com/internal/token"
)

var log = event.Log

// Services etcd 服务发现到的 Chat Server 地址表（host:port），由 server.StartListenService 每 5s 刷新。
// 沿用原有包级变量（现状保留）：登录时从中挑选一个 CS 下发 ws_addr。
var Services = make([]string, 0)

// TokenTTL 登录 token 有效期：7 天（练手项目取宽）。
const TokenTTL = 7 * 24 * time.Hour

// Server 网关 API 的依赖容器：数据访问器与 tokenizer 经构造注入。
type Server struct {
	users     *database.UserStore
	friends   *database.FriendStore
	convs     *database.ConversationStore
	msgs      *database.MessageStore
	tokenizer *token.Tokenizer
}

func NewServer(store *database.Store, authSecret string) *Server {
	return &Server{
		users:     database.NewUserStore(store),
		friends:   database.NewFriendStore(store),
		convs:     database.NewConversationStore(store),
		msgs:      database.NewMessageStore(store),
		tokenizer: token.New(authSecret),
	}
}
