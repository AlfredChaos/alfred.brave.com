package cloudware

import (
	"alfred.brave.com/cloudware/api"
	"alfred.brave.com/conf"
	"github.com/gin-gonic/gin"
)

func registerRoutes(router *gin.Engine, config *conf.Config) {
	// JSON-REST API Version 1
	v1 := router.Group(config.BaseUri(""))

	{
		api.ServiceRegister(v1)
	}
}
