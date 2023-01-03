package api

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

func Logout(router *gin.RouterGroup) {

	router.POST("/logout", func(c *gin.Context) {

		c.JSON(http.StatusOK, nil)
	})
}
