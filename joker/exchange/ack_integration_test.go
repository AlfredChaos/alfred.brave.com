//go:build integration

// 集成测试：真实 Kafka 上 ack 广播——非本机发送者跳过，本机发送者收到 {cmd:ack} 帧。
package exchange

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"alfred.brave.com/internal/chat"

	"github.com/segmentio/kafka-go"
)

// TestConsumeAcksIntegration produce chat.ack → CS 消费 → 本机发送者收到帧；无关 ack 跳过。
func TestConsumeAcksIntegration(t *testing.T) {
	brokers := []string{"127.0.0.1:9092"}
	m := NewManager()
	sender := NewClient(m, "ack-sender-1", nil)
	m.EventRegister(sender)
	other := NewClient(m, "ack-other-1", nil)
	m.EventRegister(other)

	group := fmt.Sprintf("cs-ack-it-%d", time.Now().UnixNano())
	reader := kafka.NewReader(kafka.ReaderConfig{Brokers: brokers, GroupID: group, Topic: chat.TopicAck})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- ConsumeAcks(ctx, m, reader) }()

	// 两条 ack：本机发送者 + 无关用户
	producer := &kafka.Writer{
		Addr: kafka.TCP(brokers...), Topic: chat.TopicAck, Balancer: &kafka.Hash{},
	}
	defer producer.Close()
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("produce: %v", err)
		}
	}
	raw1, _ := json.Marshal(chat.Ack{MsgID: "m-1", CliMsgID: "c-1", ConvID: "conv", Seq: 1, FromUID: "ack-sender-1", ToUID: "rcv"})
	must(producer.WriteMessages(ctx, kafka.Message{Key: []byte("ack-sender-1"), Value: raw1}))
	raw2, _ := json.Marshal(chat.Ack{MsgID: "m-2", CliMsgID: "c-2", ConvID: "conv", Seq: 2, FromUID: "someone-else", ToUID: "rcv"})
	must(producer.WriteMessages(ctx, kafka.Message{Key: []byte("someone-else"), Value: raw2}))

	// 发送者应收到 m-1 的 ack；other 不应收到任何帧
	select {
	case raw := <-sender.Send:
		var frame AckFrame
		if err := json.Unmarshal(raw, &frame); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if frame.Cmd != "ack" || frame.Data.CliMsgID != "c-1" || frame.Data.MsgID != "m-1" {
			t.Fatalf("ack frame mismatch: %+v", frame)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("sender did not receive ack frame")
	}
	select {
	case raw := <-other.Send:
		t.Fatalf("other must not receive frames, got %s", raw)
	case <-time.After(300 * time.Millisecond):
	}

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("consumer did not stop")
	}
}
