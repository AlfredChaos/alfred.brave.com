package joker

import (
	"alfred.brave.com/conf"
	"alfred.brave.com/joker/api"
	"github.com/gin-gonic/gin"
)

func registerRoutes(router *gin.Engine, config *conf.Config) {
	// JSON-REST API Version 1
	v1 := router.Group(config.BaseUri(""))

	{
		api.Login(v1)
		api.Logout(v1)
		api.SendMessage(v1)
	}
}
