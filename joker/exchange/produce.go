package exchange

import (
	"context"

	"alfred.brave.com/internal/chat"
)

// MsgProducer chat.msg 生产者抽象（D02：CS 只 produce，不做路由/落库）。
type MsgProducer interface {
	Produce(ctx context.Context, env *chat.Msg) error
}

// SetMsgProducer 注入 chat.msg 生产者（joker.Start 启动期一次；nil = 本地无 Kafka 模式）。
func (m *Manager) SetMsgProducer(p MsgProducer) {
	m.msgProducer = p
}

var errNoPipeline = &pipelineUnavailable{}

type pipelineUnavailable struct{}

func (*pipelineUnavailable) Error() string { return "message pipeline (kafka) unavailable" }

// ProduceMsg 经注入的生产者发送 chat.msg；未配置 Kafka 时显式失败而非静默丢弃。
func (m *Manager) ProduceMsg(ctx context.Context, env *chat.Msg) error {
	if m.msgProducer == nil {
		return errNoPipeline
	}
	return m.msgProducer.Produce(ctx, env)
}
