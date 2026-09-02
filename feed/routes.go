package feed

import (
	"alfred.brave.com/conf"
	"alfred.brave.com/feed/api"
	"github.com/gin-gonic/gin"
)

func registerRoutes(router *gin.Engine, config *conf.Config) {
	// 与网关同构的 /v1 前缀（网关透明转发不重写路径）
	v1 := router.Group("/v1")
	srv := api.NewServer(config.Db(), config.AuthSecret(), config.KafkaBrokers())
	api.RegisterRoutes(v1, srv)
}
