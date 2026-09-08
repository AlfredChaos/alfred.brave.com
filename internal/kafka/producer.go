// Package kafka kafka-go 封装：Producer（带 key 写入）与 topic 巡检工具。
// 纯 Go 实现无 cgo（D22 选型 kafka-go 的初衷），生产可替换 franz-go 而不动业务层。
package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"alfred.brave.com/database"
	"alfred.brave.com/event"
	"alfred.brave.com/internal/chat"

	"github.com/segmentio/kafka-go"
)

var log = event.Log

// Producer 单 topic 写入器。并发安全（kafka-go Writer 内部排队）。
// D20 环节①：调用方必须传业务 key（同 key 同分区），本封装不提供无 key 写入。
type Producer struct {
	writer *kafka.Writer
	topic  string
}

func NewProducer(brokers []string, topic string) *Producer {
	return &Producer{
		topic: topic,
		writer: &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Topic:        topic,
			Balancer:     &kafka.Hash{}, // 按 key 哈希选分区
			RequiredAcks: kafka.RequireAll,
			BatchTimeout: 5 * time.Millisecond, // 低延迟批量折中
			// 幂等生产者：kafka-go 经 Writer.Compression 由 broker 侧去重配置配合；
			// at-least-once 语义由消费端幂等兜底（D20 备案）
		},
	}
}

// Write 写一条带 key 的 JSON 消息。
func (p *Producer) Write(ctx context.Context, key string, value []byte) error {
	return p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(key),
		Value: value,
	})
}

// WriteBatch 批量写（S3c 批量化改造）：一次 WriteMessages 摊薄
// acks=all 的跨机副本确认延迟——单条路径实测 ~8ms/条的固定税，
// 批 N 条只付一次（kafka-go Writer 内部按批 flush）。
func (p *Producer) WriteBatch(ctx context.Context, msgs []kafka.Message) error {
	if len(msgs) == 0 {
		return nil
	}
	return p.writer.WriteMessages(ctx, msgs...)
}

func (p *Producer) Close() error {
	return p.writer.Close()
}

// EnsureTopics 按规范分区数建 topic（已存在则跳过）。本地/测试环境初始化用；
// 生产 compose 由 init 容器显式创建。
func EnsureTopics(brokers []string, topics map[string]int) error {
	conn, err := kafka.Dial("tcp", brokers[0])
	if err != nil {
		return fmt.Errorf("kafka dial %v: %w", brokers, err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("kafka controller: %w", err)
	}
	cconn, err := kafka.Dial("tcp", fmt.Sprintf("%s:%d", controller.Host, controller.Port))
	if err != nil {
		return fmt.Errorf("kafka dial controller: %w", err)
	}
	defer cconn.Close()

	existing := make(map[string]int)
	list, err := cconn.ReadPartitions()
	if err == nil {
		for _, p := range list {
			existing[p.Topic]++
		}
	}

	for topic, parts := range topics {
		if _, ok := existing[topic]; ok {
			continue
		}
		err := cconn.CreateTopics(kafka.TopicConfig{
			Topic:             topic,
			NumPartitions:     parts,
			ReplicationFactor: 1, // 本地单 broker；生产 RF=3 由部署侧覆盖
		})
		if err != nil {
			log.Warnf("create topic %s: %v (may already exist)", topic, err)
		} else {
			log.Infof("created kafka topic %s (%dP)", topic, parts)
		}
	}
	return nil
}

// MsgProducer 适配器：把通用 Producer 适配为 chat.msg 生产者（json 序列化 + key=conv_id）。
type MsgProducer struct {
	p *Producer
}

// NewMsgProducer chat.msg 生产者。key=conv_id 保证同会话同分区（D20 环节①）；
// conv_id 为空（首条消息）时 key 退化为空串——落库侧按成员对兜底，seq 仍保序。
func NewMsgProducer(brokers []string) *MsgProducer {
	return &MsgProducer{p: NewProducer(brokers, chat.TopicMsg)}
}

func (m *MsgProducer) Produce(ctx context.Context, env *chat.Msg) error {
	raw, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("marshal chat.msg: %w", err)
	}
	// 分区键 = conv_id；单聊首条 conv 为空（客户端常态）时退化为成员对 single_key：
	// 同会话双向消息哈希到同一分区（排序保证 A→B 与 B→A 同键），且键空间为
	// 全量会话而不是空串——S3c 实测空键把所有流量压到单热分区（12 消费者 1 干活），
	// 分片消费被结构性锁死；单键改双键后消息按会话摊开（收益见 s3c-optimization.md）。
	key := env.ConvID
	if key == "" && env.Type == chat.TypeSingle {
		key = database.SingleKey(env.FromUID, env.ToUID)
	}
	return m.p.Write(ctx, key, raw)
}

func (m *MsgProducer) Close() error {
	return m.p.Close()
}
