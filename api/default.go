package api

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

func DefaultIndex(router *gin.RouterGroup) {
	router.GET("/index", func(c *gin.Context) {
		c.HTML(http.StatusOK, "../views/index.html", gin.H{})
	})
}
