package relay

import (
	"context"
	"encoding/json"
	"testing"

	"alfred.brave.com/internal/chat"
	"alfred.brave.com/joker/exchange"
	"alfred.brave.com/joker/proto"
)

func newTestManagerWith(t *testing.T, uids ...string) *exchange.Manager {
	t.Helper()
	m := exchange.NewManager()
	for _, uid := range uids {
		c := exchange.NewClient(m, uid, nil)
		m.EventRegister(c)
	}
	return m
}

func msgReq(id, to string) *jokerproto.RelayMessageRequest {
	return &jokerproto.RelayMessageRequest{MsgId: id, CliMsgId: "c-" + id, ToUid: to,
		FromUid: "from", ConvId: "conv", Type: chat.TypeSingle, Content: []byte(`{"text":"x"}`), Seq: 1}
}

// TestBatchRelayOrderAndPartialResult 批内按请求顺序逐条入队；不存在的目标返回
// not_found 且不阻碍其余消息；results 与 messages 下标一一对应。
func TestBatchRelayOrderAndPartialResult(t *testing.T) {
	m := newTestManagerWith(t, "u1", "u2")
	s := NewServer(m)

	// 占满 u1 的 Send（容量 32）：第 33 条必须立即 send_full，不得阻塞批处理
	for i := 0; i < 32; i++ {
		c := m.GetClient("u1")
		select {
		case c.Send <- []byte("fill"):
		default:
			t.Fatalf("fill %d failed unexpectedly", i)
		}
	}
	resp, err := s.BatchRelayMessages(context.Background(), &jokerproto.BatchRelayMessagesRequest{
		Messages: []*jokerproto.RelayMessageRequest{msgReq("m1", "u1"), msgReq("m2", "nobody"), msgReq("m3", "u2")},
	})
	if err != nil {
		t.Fatalf("batch: %v", err)
	}
	if len(resp.Results) != 3 {
		t.Fatalf("results = %d, want 3", len(resp.Results))
	}
	if resp.Results[0].Delivered || resp.Results[0].Reason != "send_full" {
		t.Fatalf("m1 want send_full, got %+v", resp.Results[0])
	}
	if resp.Results[1].Delivered || resp.Results[1].Reason != "not_found" {
		t.Fatalf("m2 want not_found, got %+v", resp.Results[1])
	}
	if !resp.Results[2].Delivered {
		t.Fatalf("m3 must be delivered, got %+v", resp.Results[2])
	}
	stats := s.Stats()
	if stats.SendFull != 1 || stats.NotFound != 1 || stats.MessageEnqueued != 1 || stats.MessageBatches != 1 {
		t.Fatalf("stats mismatch: %+v", stats)
	}
	// u2 收到的是 cmd=msg 帧，seq 透传
	raw := <-m.GetClient("u2").Send
	var frame DeliveryFrame
	if err := json.Unmarshal(raw, &frame); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if frame.Cmd != "msg" || frame.Data.MsgID != "m3" {
		t.Fatalf("frame mismatch: %+v", frame)
	}
}

// TestBatchRelayAcksFrameType ACK 必须输出 cmd=ack 帧（复用消息帧会把回执当消息渲染）。
func TestBatchRelayAcksFrameType(t *testing.T) {
	m := newTestManagerWith(t, "sender")
	s := NewServer(m)

	resp, err := s.BatchRelayAcks(context.Background(), &jokerproto.BatchRelayAcksRequest{
		Acks: []*jokerproto.RelayAckRequest{{MsgId: "m1", CliMsgId: "c1", FromUid: "sender", ToUid: "rcv", Seq: 7}},
	})
	if err != nil {
		t.Fatalf("batch acks: %v", err)
	}
	if len(resp.Results) != 1 || !resp.Results[0].Delivered {
		t.Fatalf("ack result: %+v", resp.Results)
	}
	raw := <-m.GetClient("sender").Send
	var frame AckFrame
	if err := json.Unmarshal(raw, &frame); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if frame.Cmd != "ack" || frame.Data.CliMsgID != "c1" || frame.Data.Seq != 7 {
		t.Fatalf("ack frame mismatch: %+v", frame)
	}
	if stats := s.Stats(); stats.AckEnqueued != 1 || stats.AckBatches != 1 {
		t.Fatalf("ack stats: %+v", stats)
	}
}
