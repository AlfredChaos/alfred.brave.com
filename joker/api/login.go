package api

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

func Login(router *gin.RouterGroup) {

	router.POST("/login", func(c *gin.Context) {

		c.JSON(http.StatusOK, nil)
	})
}
