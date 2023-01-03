package api

import (
	"bytes"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

type Client struct {
	conn *websocket.Conn
	send chan []byte
}

var (
	newline = []byte{'\n'}
	space   = []byte{' '}
)

// 出于幂等和不改变服务器状态的原则，login本应使用GET方法，但brave暂未支持https所以无法保证安全性
// 暂时使用POST方法传输
func WebSocket(router *gin.RouterGroup) {

	router.GET("/ws", func(c *gin.Context) {
		log.Infof("ready upgrade websocket")
		UpgradeWebsockets(c)
	})
}

// 结果：获取websocket host
// 过程：
// 1、从服务注册与发现的joker中随机找一个发送登陆请求
// 2、joker返回正确的wsConn
func UpgradeWebsockets(c *gin.Context) {
	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.Errorf("%v", err)
		return
	}
	client := &Client{conn: conn, send: make(chan []byte, 256)}
	go client.writePump()
	go client.readPump()
}

func (c *Client) readPump() {
	defer func() { c.conn.Close() }()
	c.conn.SetReadLimit(512)
	c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})
	for {
		_, message, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Errorf("error: %v", err)
			}
			break
		}
		message = bytes.TrimSpace(bytes.Replace(message, newline, space, -1))
		c.send <- message
		log.Debugf("==> Get Message: %s", message)
	}
}

func (c *Client) writePump() {
	ticker := time.NewTicker(1)
	defer func() { ticker.Stop(); c.conn.Close() }()
	for {
		select {
		case message, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if !ok {
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			w, err := c.conn.NextWriter(websocket.TextMessage)
			if err != nil {
				return
			}
			w.Write(message)

			n := len(c.send)
			for i := 0; i < n; i++ {
				w.Write(newline)
				w.Write(<-c.send)
			}

			if err := w.Close(); err != nil {
				return
			}
		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}
