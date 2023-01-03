package api

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

func ServiceList(router *gin.RouterGroup) {

	router.GET("/services/:name", func(c *gin.Context) {

		c.JSON(http.StatusOK, nil)
	})
}
