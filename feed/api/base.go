// Package feed/api 朋友圈 API handler 层（鉴权与业务）。网关转发与本服务直连共用。
package api

import (
	"alfred.brave.com/database"
	"alfred.brave.com/event"
	"alfred.brave.com/internal/chat"
	"alfred.brave.com/internal/kafka"
	"alfred.brave.com/internal/token"
)

var log = event.Log

// Server feed API 依赖容器（构造注入）。
type Server struct {
	users     *database.UserStore
	friends   *database.FriendStore
	feeds     *database.FeedStore
	fanout    *kafka.Producer // feed.fanout 生产者
	tokenizer *token.Tokenizer
}

func NewServer(store *database.Store, authSecret string, brokers []string) *Server {
	return &Server{
		users:     database.NewUserStore(store),
		friends:   database.NewFriendStore(store),
		feeds:     database.NewFeedStore(store),
		fanout:    kafka.NewProducer(brokers, chat.TopicFanout),
		tokenizer: token.New(authSecret),
	}
}
