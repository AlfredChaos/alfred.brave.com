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
	// 网关(:37001)与 Chat Server(:37002) 端口分离部署下，网页客户端的 WS 连接
	// 必然跨源（跨端口即跨源），gorilla 默认 CheckOrigin 会全部拒绝——浏览器
	// 实测暴露（此前 e2e 用 Go 客户端不带 Origin 头，测不出该问题）。
	// 当前连接信任模型是 URL 中的 uid（测试客户端），Origin 不作为安全边界；
	// 若对外开放需升级为 token 握手 + Origin 白名单（见 docs 面试取舍说明）。
	CheckOrigin: func(r *http.Request) bool { return true },
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
