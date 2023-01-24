package api

import (
	"net/http"

	"alfred.brave.com/joker/exchange"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

func Websocket(router *gin.RouterGroup) {

	router.GET("/ws/:id", func(c *gin.Context) {
		user := c.Param("id")
		UpgradeWebsockets(user, c)
		c.JSON(http.StatusOK, nil)
	})
}

func UpgradeWebsockets(user string, c *gin.Context) {
	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.Errorf("upgrade http to websocket error = %v", err)
		return
	}
	client := exchange.NewClient(user, conn)

	go client.ReadPump()
	go client.WritePump()

	// 注册至manager
	exchange.Controller.Register <- client
}
