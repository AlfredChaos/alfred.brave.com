package exchange

import (
	"context"
	"encoding/json"
	"testing"

	"alfred.brave.com/internal/chat"
)

// fakeMsgProducer 记录 produce 调用。
type fakeMsgProducer struct {
	envs []*chat.Msg
	err  error
}

func (f *fakeMsgProducer) Produce(ctx context.Context, env *chat.Msg) error {
	if f.err != nil {
		return f.err
	}
	f.envs = append(f.envs, env)
	return nil
}

// TestHandleMsgProduces 从连接发消息：env.from 必须取连接身份（不信任客户端），
// key 字段（conv_id）与载荷透传正确。
func TestHandleMsgProduces(t *testing.T) {
	fake := &fakeMsgProducer{}
	m := NewManager()
	m.SetMsgProducer(fake)

	c := NewClient(m, "u1", nil)
	frame := `{"conv_id":"conv-9","to_uid":"u2","cli_msg_id":"c-1","content":{"text":"hi"}}`
	handleMsg(c, []byte(frame))

	resp := readResponse(t, c)
	if resp.Code != OK {
		t.Fatalf("resp code = %d (%s), want %d", resp.Code, resp.CodeMsg, OK)
	}
	if len(fake.envs) != 1 {
		t.Fatalf("produce called %d times, want 1", len(fake.envs))
	}
	env := fake.envs[0]
	if env.FromUID != "u1" || env.ToUID != "u2" || env.ConvID != "conv-9" || env.CliMsgID != "c-1" {
		t.Fatalf("env mismatch: %+v", env)
	}
}

// TestHandleMsgNoPipeline 未配置 Kafka 时显式拒绝（不静默丢消息）。
func TestHandleMsgNoPipeline(t *testing.T) {
	m := NewManager() // 无 producer
	c := NewClient(m, "u1", nil)
	handleMsg(c, []byte(`{"to_uid":"u2","content":{"text":"hi"}}`))
	if resp := readResponse(t, c); resp.Code != OperationFailure {
		t.Fatalf("resp code = %d, want %d (OperationFailure)", resp.Code, OperationFailure)
	}
}

// TestQuickCheckContent CS 快检：mentions 超 50 拒绝、非法 JSON 拒绝、正常放行。
func TestQuickCheckContent(t *testing.T) {
	if err := quickCheckContent(json.RawMessage(`{"text":"hi"}`)); err != nil {
		t.Fatalf("plain content rejected: %v", err)
	}
	mentions := `{"mentions":[`
	for i := 0; i < 51; i++ {
		if i > 0 {
			mentions += ","
		}
		mentions += `"u"`
	}
	mentions += `]}`
	if err := quickCheckContent(json.RawMessage(mentions)); err == nil {
		t.Fatal("51 mentions must be rejected")
	}
	if err := quickCheckContent(json.RawMessage(`{"mentions":["a","b"],"mention_all":false}`)); err != nil {
		t.Fatalf("valid mentions rejected: %v", err)
	}
	if err := quickCheckContent(json.RawMessage(`not-json`)); err == nil {
		t.Fatal("invalid json must be rejected")
	}
}
