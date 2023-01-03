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
	templatePaht := fmt.Sprintf("%s/%s", projectPath, "template/*")
	// JSON-REST API Version 1
	v1 := router.Group(config.BaseUri(""))
	router.LoadHTMLGlob(templatePaht)

	{
		api.DefaultIndex(v1)
		api.Register(v1)
		api.GetUser(v1)
		api.Login(v1)
		api.WebSocket(v1)
	}
}
