package api

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

func ServiceRandomGet(router *gin.RouterGroup) {

	router.GET("/services", func(c *gin.Context) {

		c.JSON(http.StatusOK, nil)
	})
}
