package api

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

func SendMessage(router *gin.RouterGroup) {

	router.POST("/messages", func(c *gin.Context) {

		c.JSON(http.StatusOK, nil)
	})
}
