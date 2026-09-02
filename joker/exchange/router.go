package exchange

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"alfred.brave.com/internal/chat"
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

// MsgFrame cmd=msg 的业务载荷（§3 步骤 1）。
type MsgFrame struct {
	ConvID   string          `json:"conv_id"` // 单聊首条可空；群聊=gid
	ToUID    string          `json:"to_uid"`  // 群聊可空（扇出由 persist 按成员展开）
	CliMsgID string          `json:"cli_msg_id"`
	Group    bool            `json:"group"` // true=群聊（type=group）
	Content  json.RawMessage `json:"content"`
}

// handleMsg 消息入口（§3 步骤 1-2）：CS 是哑管道——快检后组 chat.msg produce，
// 不做路由/落库/投递（D01/D02/D18）。from 取连接身份，不信任客户端上报。
func handleMsg(c *Client, data json.RawMessage) {
	frame := &MsgFrame{}
	if err := json.Unmarshal(data, frame); err != nil {
		log.Errorf("msg frame unmarshal error = %v", err)
		c.SendResponse(ParameterIllegal, "", nil)
		return
	}
	if (frame.ToUID == "" && !frame.Group) || len(frame.Content) == 0 {
		c.SendResponse(ParameterIllegal, "to_uid/content required", nil)
		return
	}
	if frame.Group && frame.ConvID == "" {
		c.SendResponse(ParameterIllegal, "group msg requires conv_id(gid)", nil)
		return
	}
	if err := quickCheckContent(frame.Content); err != nil {
		c.SendResponse(ParameterIllegal, err.Error(), nil)
		return
	}

	// 从 content 提取 @ 元数据进信封（服务端解析，客户端无法伪造信封与内容不一致）；
	// 群聊的权威校验（@all→owner、mentions⊆成员）在 persist 事务内（T15）
	var meta struct {
		Mentions   []string `json:"mentions"`
		MentionAll bool     `json:"mention_all"`
	}
	_ = json.Unmarshal(frame.Content, &meta)

	env := &chat.Msg{
		ConvID:     frame.ConvID,
		CliMsgID:   frame.CliMsgID,
		FromUID:    c.UserId,
		ToUID:      frame.ToUID,
		Type:       chat.TypeSingle,
		Content:    json.RawMessage(frame.Content),
		Mentions:   meta.Mentions,
		MentionAll: meta.MentionAll,
		SentAt:     time.Now().Unix(),
	}
	if frame.Group {
		env.Type = chat.TypeGroup
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.manager.ProduceMsg(ctx, env); err != nil {
		log.Errorf("produce chat.msg from %s to %s failed: %v", c.UserId, frame.ToUID, err)
		c.SendResponse(OperationFailure, "message pipeline unavailable", nil)
		return
	}
	// 200 = 已受理入队；送达回执走 chat.ack（T07），不是这里的 200
	c.SendResponse(OK, "accepted", nil)
}

// quickCheckContent CS 格式快检（§5 @ 校验链的 CS 段）：mentions ≤50、mention_all 布尔。
// 权威校验在 persist 事务内（T15 扩展群聊语义）。
func quickCheckContent(content json.RawMessage) error {
	var c struct {
		Mentions   []string `json:"mentions"`
		MentionAll *bool    `json:"mention_all"`
	}
	if err := json.Unmarshal(content, &c); err != nil {
		return errors.New("content must be valid json object")
	}
	if len(c.Mentions) > 50 {
		return errors.New("mentions exceed 50")
	}
	return nil
}
