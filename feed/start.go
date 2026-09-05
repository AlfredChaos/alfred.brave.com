// Package feed 朋友圈 API 服务（brave feed :37003，§6 写路径入口）。
// 发布：同步落库返回 post_id + 异步 produce feed.fanout；读取（inbox+pull merge）在 T11。
// 无状态可多副本，经网关 /v1/feed/* 透明转发，也直接对外提供同构 API。
package feed

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"alfred.brave.com/conf"
	"alfred.brave.com/event"
	"alfred.brave.com/internal/etcd"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

var log = event.Log

// ServiceId 本实例标识（etcd 注册 key）。
var ServiceId = uuid.NewString()

func Start(ctx context.Context, config *conf.Config) {
	defer func() {
		if err := recover(); err != nil {
			log.Error(err)
		}
	}()

	start := time.Now()
	router := gin.New()
	registerRoutes(router, config)

	ser := &http.Server{
		Addr:    fmt.Sprintf("%s:%d", config.GetHttpHost(), config.GetHttpPort()),
		Handler: router,
	}
	log.Infof("feed: listening on %s [%s]", ser.Addr, time.Since(start))
	go func() {
		if err := ser.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Errorf("feed: %s", err)
		}
	}()
	go ServiceRegister(ctx, config)

	<-ctx.Done()
	log.Info("feed: shutting down")
	if err := ser.Close(); err != nil {
		log.Errorf("feed: shutdown failed (%s)", err)
	}
}

// ServiceRegister 注册进 etcd（kind=feed），供网关发现与转发。
func ServiceRegister(cctx context.Context, config *conf.Config) {
	lease := int64(60)
	host := fmt.Sprintf("%s:%d", config.GetAdvertiseHost(), config.GetAdvertisePort())
	server, err := etcd.NewServiceRegister(etcd.KindFeed, ServiceId, host, lease, config.EtcdClient)
	if err != nil {
		// 首次注册失败不放弃：etcd 可能晚于 feed 就绪，Run 维持循环会退避重试
		log.Warnf("feed service %s initial register failed: %v (will retry)", ServiceId, err)
	}
	defer server.Close()
	go server.Run(cctx)
	<-cctx.Done()
}
