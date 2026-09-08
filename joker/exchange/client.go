package exchange

import (
	"bytes"
	"encoding/json"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"alfred.brave.com/event"
	"github.com/gorilla/websocket"
)

var log = event.Log

var (
	newline = []byte{'\n'}
	space   = []byte{' '}
)

// HeartbeatExpiration 心跳超时阈值：客户端 30s 一报，6 分钟无任何帧视为离线（D14）。
// 参考 gowebsocket heartbeatExpirationTime = 6*60。
const HeartbeatExpiration = 6 * time.Minute

// Client 单个 WebSocket 连接。lastActive 原子更新（ReadPump 写、清理任务读），无锁。
type Client struct {
	UserId string
	Socket *websocket.Conn
	Send   chan []byte

	manager    *Manager
	lastActive atomic.Int64 // 最近一次收到客户端帧的 unixnano
	closeOnce  sync.Once    // 清理任务与 Pump 退出可能并发触发 Close，必须幂等
}

type MessageResponse struct {
	Code    uint32      `json:"code"`
	CodeMsg string      `json:"code_msg"`
	Message interface{} `json:"message,omitempty"`
}

// WsFrame WebSocket 统一帧协议：cmd 分发到注册式路由，data 为各 cmd 的业务载荷。
type WsFrame struct {
	Cmd  string          `json:"cmd"`
	Data json.RawMessage `json:"data,omitempty"`
}

func NewClient(manager *Manager, userId string, socket *websocket.Conn) *Client {
	c := &Client{
		UserId: userId,
		Socket: socket,
		// Send 缓冲 32：S1 真机测出 make(chan []byte, 1000) 是每连接 ~24KB 预分配税
		// （1000×24B 切片头，零消息也占；135k 连接 ≈ 3.1GB，占 cs 活堆 ~40%）。
		// 缩到 32 不改变反压语义：三个跨协程写点（ack=丢+seq 兜底 / broadcast=踢线 /
		// relay=立即 send_full 回未投递）均为 select+显式路径，慢消费踢线而非堆积（D20 环节④）。
		// 唯一裸写 Send 的 SendResponse 走自身通道自循环，最坏自锁单连接，6 分钟心跳清理兜底。
		Send:    make(chan []byte, 32),
		manager: manager,
	}
	c.Touch()
	return c
}

// Touch 记录一次客户端活动（心跳或任意业务帧均刷新存活时间，D14 被动模式）。
func (c *Client) Touch() {
	c.TouchAt(time.Now())
}

// TouchAt 以指定时间刷新活跃时间戳（测试可注入时钟）。
func (c *Client) TouchAt(t time.Time) {
	c.lastActive.Store(t.UnixNano())
}

// Close 幂等关闭连接：关 Send 通知 WritePump 退出，关 Socket 解阻塞 ReadPump。
func (c *Client) Close() {
	c.closeOnce.Do(func() {
		close(c.Send)
		if c.Socket != nil {
			if err := c.Socket.Close(); err != nil {
				log.Errorf("user %s close socket: %v", c.UserId, err)
			}
		}
	})
}

// WritePump 只负责把 Send 通道的消息写到 socket。
// D14：服务端不再主动 ping（原 ticker 1ns 是心跳风暴 bug），存活检测交给
// 客户端心跳 + Manager 定时清理 + ReadPump 读超时三重被动机制。
func (c *Client) WritePump() {
	defer func() {
		if r := recover(); r != nil {
			log.Errorf("User %s WritePump Stop: %s, %s", c.UserId, string(debug.Stack()), r)
		}
	}()

	defer func() {
		if c.Socket != nil {
			c.Socket.Close()
		}
	}()
	for {
		message, ok := <-c.Send
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

		// 批量写：把通道里积压的消息合并到一帧，减少系统调用
		n := len(c.Send)
		for i := 0; i < n; i++ {
			w.Write(newline)
			w.Write(<-c.Send)
		}

		if err := w.Close(); err != nil {
			return
		}
	}
}

// ReadPump 唯一的 socket 读循环。退出时负责把连接从 Manager 注销。
func (c *Client) ReadPump() {
	defer func() {
		if r := recover(); r != nil {
			log.Errorf("User %s ReadPump Stop: %s, %s", c.UserId, string(debug.Stack()), r)
		}
	}()

	defer func() {
		if c.Socket != nil {
			c.Socket.Close()
		}
		// 生命周期闭环：读循环退出 = 连接死亡，必须从连接表移除（原代码从不触发 Unregister）
		if c.manager != nil {
			c.manager.Unregister <- c
		}
	}()
	c.Socket.SetReadLimit(512)
	// 读超时是心跳清理的 TCP 层兜底：6 分钟内没有任何入站帧（含 pong）即断开
	c.Socket.SetReadDeadline(time.Now().Add(HeartbeatExpiration))
	c.Socket.SetPongHandler(func(string) error {
		c.Socket.SetReadDeadline(time.Now().Add(HeartbeatExpiration))
		return nil
	})
	for {
		_, message, err := c.Socket.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Errorf("user %s read error: %v", c.UserId, err)
			}
			return
		}
		// 任何入站帧都视为存活信号并刷新读超时（D14 被动模式）
		c.Touch()
		c.Socket.SetReadDeadline(time.Now().Add(HeartbeatExpiration))
		message = bytes.TrimSpace(bytes.Replace(message, newline, space, -1))
		log.Debugf("==> Get Message: %s", message)
		poccessMessage(c, message)
	}
}

func (c *Client) SendResponse(code uint32, codeMsg string, message interface{}) {
	resp := &MessageResponse{Code: code, CodeMsg: getErrorMessage(code, codeMsg), Message: message}
	respByte, err := json.Marshal(resp)
	if err != nil {
		log.Errorf("send response error = %v", err)
		return
	}
	c.Send <- respByte
}
