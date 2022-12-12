package api

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
)

type Hello struct {
	Name string `json:"name"`
	ID   int    `json:"id"`
	Auth bool   `json:"auth"`
}

func GetHello(router *gin.RouterGroup) {
	router.GET("/hello", func(c *gin.Context) {

		fmt.Println("Hello, here is brave... Welcome to the greatest system")

		a := &Hello{
			Name: "brave",
			ID:   1,
			Auth: true,
		}

		c.JSON(http.StatusOK, a)
	})
}
