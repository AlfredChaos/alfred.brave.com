package server

import (
	"fmt"
	"os"

	"alfred.brave.com/conf"
	"alfred.brave.com/server/api"
	"github.com/gin-gonic/gin"
)

func registerRoutes(router *gin.Engine, config *conf.Config) {
	projectPath := os.Getenv("PROJECT_PATH")
	templatePath := fmt.Sprintf("%s/%s", projectPath, "template/*")
	// JSON-REST API Version 1（显式 /v1 前缀；HTML 页面留根路径）
	v1 := router.Group("/v1")
	router.LoadHTMLGlob(templatePath)

	srv := api.NewServer(config.Db(), config.AuthSecret(), config.KafkaBrokers(), config.SnowflakeWorkerID())
	{
		api.DefaultIndex(v1)
		srv.Register(v1)
		srv.Login(v1)
		srv.GetUser(v1)
		srv.Users(v1)
		srv.Friends(v1)
		srv.Conversations(v1)
		srv.Groups(v1)
		srv.FeedProxy(v1)
	}
}
