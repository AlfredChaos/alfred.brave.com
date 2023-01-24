package exchange

import (
	"bytes"
	"runtime/debug"
	"time"

	"encoding/json"

	"alfred.brave.com/event"
	"alfred.brave.com/internal/etcd"
	"github.com/gorilla/websocket"
)

var log = event.Log

var (
	newline = []byte{'\n'}
	space   = []byte{' '}
)

type Client struct {
	UserId string
	Socket *websocket.Conn
	Send   chan []byte
}

type MessageRequest struct {
	From    string      `json:"from"`
	To      string      `json:"to"`
	Message interface{} `json:"message,omitempty"`
}

type MessageResponse struct {
	Code    uint32      `json:"code"`
	CodeMsg string      `json:"code_msg"`
	Message interface{} `json:"message,omitempty"`
}

func NewClient(userId string, socket *websocket.Conn) *Client {
	return &Client{
		UserId: userId,
		Socket: socket,
		Send:   make(chan []byte, 1000),
	}
}

func (c *Client) WritePump() {
	defer func() {
		if r := recover(); r != nil {
			log.Errorf("User %s WritePump Stop: %s, %s", c.UserId, string(debug.Stack()), r)
		}
	}()

	ticker := time.NewTicker(1)
	defer func() { ticker.Stop(); c.Socket.Close() }()
	for {
		select {
		case message, ok := <-c.Send:
			c.Socket.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if !ok {
				c.Socket.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			w, err := c.Socket.NextWriter(websocket.TextMessage)
			if err != nil {
				return
			}
			w.Write(message)

			n := len(c.Send)
			for i := 0; i < n; i++ {
				w.Write(newline)
				w.Write(<-c.Send)
			}

			if err := w.Close(); err != nil {
				return
			}
		case <-ticker.C:
			c.Socket.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.Socket.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func (c *Client) ReadPump() {
	defer func() {
		if r := recover(); r != nil {
			log.Errorf("User %s ReadPump Stop: %s, %s", c.UserId, string(debug.Stack()), r)
		}
	}()

	defer func() { c.Socket.Close() }()
	c.Socket.SetReadLimit(512)
	c.Socket.SetReadDeadline(time.Now().Add(60 * time.Second))
	c.Socket.SetPongHandler(func(string) error {
		c.Socket.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})
	for {
		_, message, err := c.Socket.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Errorf("error: %v", err)
			}
			break
		}
		message = bytes.TrimSpace(bytes.Replace(message, newline, space, -1))
		log.Debugf("==> Get Message: %s", message)
		poccessMessage(c, message)
	}
}

func (c *Client) SendMessage(request *MessageRequest) {
	answer := request.To
	// 判断收信人是否在线
	userFactory := etcd.UserFactory{
		Namespace: etcd.PrefixUsers,
		User:      &etcd.User{UserId: answer},
	}
	if err := userFactory.Get(); err != nil {
		log.Errorf("check answer %s login status fail, err = %v", answer, err)
		c.SendResponse(NotLoggedIn, "", nil)
		return
	}
	if ServiceHost == userFactory.User.LoginHost {
		// 收信人在本地登录
	} else {
		// 收信人在异地登录
	}
}

func (c *Client) SendResponse(code uint32, codeMsg string, message interface{}) {
	resp := &MessageResponse{Code: code, CodeMsg: getErrorMessage(code, codeMsg), Message: message}
	respByte, err := json.Marshal(resp)
	if err != nil {
		log.Errorf("send respnse error = %v", err)
		return
	}
	c.Send <- respByte
}

func poccessMessage(client *Client, message []byte) {
	defer func() {
		if r := recover(); r != nil {
			log.Errorf("message process stop, recover = %v", r)
		}
	}()

	request := &MessageRequest{}
	if err := json.Unmarshal(message, request); err != nil {
		log.Errorf("process message json unmarrshal error = %v", err)
		client.SendResponse(ParameterIllegal, "", nil)
		return
	}

	client.SendMessage(request)
}
