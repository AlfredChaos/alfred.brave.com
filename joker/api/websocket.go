package api

import (
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
		// 升级后连接已被劫持，不能再写 HTTP 响应（原 c.JSON 会触发
		// "http: connection has been hijacked" panic——浏览器实测暴露）
		UpgradeWebsockets(user, c)
	})
}

func UpgradeWebsockets(user string, c *gin.Context) {
	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.Errorf("upgrade http to websocket error = %v", err)
		return
	}
	client := exchange.NewClient(exchange.Controller, user, conn)

	go client.ReadPump()
	go client.WritePump()

	// 注册至manager
	exchange.Controller.Register <- client
}
