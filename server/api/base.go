package api

import (
	"time"

	"alfred.brave.com/database"
	"alfred.brave.com/event"
	"alfred.brave.com/internal/chat"
	"alfred.brave.com/internal/kafka"
	"alfred.brave.com/internal/snowflake"
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
	groups    *database.GroupStore
	tokenizer *token.Tokenizer
	gidGen    database.GroupIDFunc // 群 ID 生成（雪花）
	chatMsg   *kafka.Producer      // 群事件入 chat.msg（D09：管理事件经 persist 统一扇出）
}

func NewServer(store *database.Store, authSecret string, brokers []string, workerID int64) *Server {
	gen, err := snowflake.New(workerID)
	if err != nil {
		// workerID 越界属部署配置错误，直接暴露
		panic(err)
	}
	return &Server{
		users:     database.NewUserStore(store),
		friends:   database.NewFriendStore(store),
		convs:     database.NewConversationStore(store),
		msgs:      database.NewMessageStore(store),
		groups:    database.NewGroupStore(store),
		tokenizer: token.New(authSecret),
		gidGen:    func() (string, error) { return gen.Next() },
		chatMsg:   kafka.NewProducer(brokers, chat.TopicMsg),
	}
}
