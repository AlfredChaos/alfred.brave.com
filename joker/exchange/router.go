package exchange

import (
	"encoding/json"
)

// 注册式消息路由（AGENTS §5.2 OCP）：新增 cmd 只需 RegisterCmd，不改分发主逻辑。
// routers 仅在包初始化阶段写入，运行期只读——不构成新的运行期可变全局。

// Handler 处理一个 cmd 的业务帧。
type Handler func(c *Client, data json.RawMessage)

var routers = make(map[string]Handler)

// RegisterCmd 注册 cmd 处理器。重复注册视为编码错误，直接告警并覆盖。
func RegisterCmd(cmd string, h Handler) {
	if _, dup := routers[cmd]; dup {
		log.Warnf("cmd %s handler re-registered, overriding", cmd)
	}
	routers[cmd] = h
}

func init() {
	RegisterCmd("login", handleLogin)
	RegisterCmd("heartbeat", handleHeartbeat)
	RegisterCmd("msg", handleMsg)
}

// poccessMessage WS 帧分发入口：解析统一帧协议后按 cmd 路由。
func poccessMessage(client *Client, message []byte) {
	defer func() {
		if r := recover(); r != nil {
			log.Errorf("message process stop, recover = %v", r)
		}
	}()

	frame := &WsFrame{}
	if err := json.Unmarshal(message, frame); err != nil {
		log.Errorf("process message json unmarshal error = %v", err)
		client.SendResponse(ParameterIllegal, "", nil)
		return
	}
	handler, ok := routers[frame.Cmd]
	if !ok {
		log.Warnf("unknown cmd %s from user %s", frame.Cmd, client.UserId)
		client.SendResponse(ParameterIllegal, "", nil)
		return
	}
	handler(client, frame.Data)
}

// handleLogin 登录占位：用户身份由 /ws/:id 提供，这里只回执确认（T03 网关化后复核）。
func handleLogin(c *Client, data json.RawMessage) {
	c.SendResponse(OK, "", nil)
}

// handleHeartbeat 心跳（D14）：只刷新存活时间戳并回执。
// 客户端约定每 30s 上报一次；6 分钟无心跳由 Manager 清理任务断开。
// 注：D15 的 seq 对账（心跳携带 last_seqs）为 P2 设计项，本阶段不实现。
func handleHeartbeat(c *Client, data json.RawMessage) {
	c.Touch()
	c.SendResponse(OK, "", nil)
}

// handleMsg 消息投递（T01 保留旧逻辑，T05 重写为 produce chat.msg）。
func handleMsg(c *Client, data json.RawMessage) {
	request := &MessageRequest{}
	if err := json.Unmarshal(data, request); err != nil {
		log.Errorf("msg frame unmarshal error = %v", err)
		c.SendResponse(ParameterIllegal, "", nil)
		return
	}
	if request.From == "" || request.To == "" {
		c.SendResponse(ParameterIllegal, "from/to required", nil)
		return
	}
	c.SendMessage(request)
}
