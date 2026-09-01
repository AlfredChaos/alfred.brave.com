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
	// JSON-REST API Version 1
	v1 := router.Group(config.BaseUri(""))
	router.LoadHTMLGlob(templatePath)

	srv := api.NewServer(config.Db())
	{
		api.DefaultIndex(v1)
		srv.Register(v1)
		srv.GetUser(v1)
		srv.Login(v1)
	}
}
