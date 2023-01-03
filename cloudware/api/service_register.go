package api

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

func ServiceRegister(router *gin.RouterGroup) {

	router.POST("/services", func(c *gin.Context) {

		c.JSON(http.StatusOK, nil)
	})
}
