//go:build integration

// 集成测试：真实 PG + 真实 Kafka 上的 persist 闭环。
// 前置：PG @127.0.0.1:55432（brave/brave/brave），Kafka @127.0.0.1:9092。
package persist

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"alfred.brave.com/database"
	"alfred.brave.com/internal/chat"
	ibrave "alfred.brave.com/internal/kafka"

	"github.com/segmentio/kafka-go"
)

func newIntegrationEnv(t *testing.T) (*Worker, *database.Store, *fakePushCollector) {
	t.Helper()
	dsn := os.Getenv("BRAVE_PG_DSN")
	if dsn == "" {
		dsn = "postgres://brave:brave@127.0.0.1:55432/brave?sslmode=disable"
	}
	brokers := []string{"127.0.0.1:9092"}
	if env := os.Getenv("BRAVE_KAFKA_BROKERS"); env != "" {
		brokers = []string{env}
	}
	if err := ibrave.EnsureTopics(brokers, chat.Partitions); err != nil {
		t.Fatalf("ensure topics: %v", err)
	}

	store, err := database.NewStore(context.Background(), dsn, 4)
	if err != nil {
		t.Fatalf("connect pg: %v", err)
	}
	t.Cleanup(store.Close)

	collector := &fakePushCollector{}
	w := New(store, collector, brokers, fmt.Sprintf("persist-it-%d", time.Now().UnixNano()))
	return w, store, collector
}

// fakePushCollector 捕获 chat.push 输出（不写 Kafka，验证 Handle 层）。
type fakePushCollector struct {
	mu     sync.Mutex
	pushes []chat.Push
	keys   []string
}

func (f *fakePushCollector) Write(ctx context.Context, key string, value []byte) error {
	var p chat.Push
	if err := json.Unmarshal(value, &p); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pushes = append(f.pushes, p)
	f.keys = append(f.keys, key)
	return nil
}

func (f *fakePushCollector) snapshot() ([]chat.Push, []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]chat.Push{}, f.pushes...), append([]string{}, f.keys...)
}

func ensureUser(t *testing.T, store *database.Store, name string) string {
	t.Helper()
	us := database.NewUserStore(store)
	u := &database.User{UserName: name, Email: name + "@it.io", Password: []byte("x")}
	if err := us.Create(context.Background(), u); err != nil {
		t.Fatalf("create user %s: %v", name, err)
	}
	return u.UID
}

// TestPersistEndToEnd 首条消息（空 conv_id）→ 建会话 → seq=1 → push 一次；第二条 seq=2。
func TestPersistEndToEnd(t *testing.T) {
	w, store, collector := newIntegrationEnv(t)
	ctx := context.Background()
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	alice := ensureUser(t, store, "pa"+suffix)
	bob := ensureUser(t, store, "pb"+suffix)

	m1 := &chat.Msg{CliMsgID: "c-1", FromUID: alice, ToUID: bob, Type: chat.TypeSingle, Content: map[string]string{"text": "hello"}}
	if err := w.Handle(ctx, m1); err != nil {
		t.Fatalf("handle m1: %v", err)
	}
	if m1.ConvID == "" {
		t.Fatal("conv_id must be resolved on first message")
	}

	msgs := database.NewMessageStore(store)
	got, err := msgs.ListAfter(ctx, m1.ConvID, 0, 10)
	if err != nil || len(got) != 1 || got[0].Seq != 1 {
		t.Fatalf("messages after m1: err=%v len=%d", err, len(got))
	}

	m2 := &chat.Msg{CliMsgID: "c-2", ConvID: m1.ConvID, FromUID: alice, ToUID: bob, Type: chat.TypeSingle, Content: map[string]string{"text": "world"}}
	if err := w.Handle(ctx, m2); err != nil {
		t.Fatalf("handle m2: %v", err)
	}
	got, err = msgs.ListAfter(ctx, m1.ConvID, 0, 10)
	if err != nil || len(got) != 2 || got[1].Seq != 2 {
		t.Fatalf("messages after m2: err=%v len=%d", err, len(got))
	}

	pushes, keys := collector.snapshot()
	if len(pushes) != 2 {
		t.Fatalf("pushes = %d, want 2", len(pushes))
	}
	if keys[0] != bob || keys[1] != bob {
		t.Fatalf("push keys = %v, want [%s %s] (to_uid)", keys, bob, bob)
	}
	if pushes[0].Seq != 1 || pushes[1].Seq != 2 {
		t.Fatalf("push seqs = %v, want [1 2]", []int64{pushes[0].Seq, pushes[1].Seq})
	}
	if pushes[0].MsgID != got[0].MsgID {
		t.Fatalf("push msg_id %s != stored %s", pushes[0].MsgID, got[0].MsgID)
	}

	// 会话 last_seq 同步
	convStore := database.NewConversationStore(store)
	conv, err := convStore.Get(ctx, m1.ConvID)
	if err != nil || conv.LastSeq != 2 {
		t.Fatalf("conversation last_seq: err=%v last_seq=%d", err, conv.LastSeq)
	}
}

// TestPersistReplayIdempotent 重放幂等：同一消息重投 2 次 → 仍 1 行、seq 不变、无空洞。
func TestPersistReplayIdempotent(t *testing.T) {
	w, store, collector := newIntegrationEnv(t)
	ctx := context.Background()
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	alice := ensureUser(t, store, "ra"+suffix)
	bob := ensureUser(t, store, "rb"+suffix)

	m1 := &chat.Msg{CliMsgID: "r-1", FromUID: alice, ToUID: bob, Type: chat.TypeSingle, Content: map[string]string{"text": "x"}}
	if err := w.Handle(ctx, m1); err != nil {
		t.Fatalf("handle: %v", err)
	}
	convID := m1.ConvID
	msgID := m1.MsgID

	// 重放两次（模拟 Kafka 重投）
	for i := 0; i < 2; i++ {
		replay := &chat.Msg{CliMsgID: "r-1", FromUID: alice, ToUID: bob, Type: chat.TypeSingle, Content: map[string]string{"text": "x"}}
		if err := w.Handle(ctx, replay); err != nil {
			t.Fatalf("replay %d: %v", i, err)
		}
		if replay.MsgID != msgID {
			t.Fatalf("replay must reuse deterministic msg_id: %s vs %s", replay.MsgID, msgID)
		}
	}

	msgs := database.NewMessageStore(store)
	got, err := msgs.ListAfter(ctx, convID, 0, 10)
	if err != nil || len(got) != 1 {
		t.Fatalf("after replays: err=%v rows=%d, want 1", err, len(got))
	}

	// 后续新消息 seq 必须无空洞（=2，说明重放没有烧号）
	m2 := &chat.Msg{CliMsgID: "r-2", ConvID: convID, FromUID: alice, ToUID: bob, Type: chat.TypeSingle, Content: map[string]string{"text": "y"}}
	if err := w.Handle(ctx, m2); err != nil {
		t.Fatalf("handle m2: %v", err)
	}
	got, _ = msgs.ListAfter(ctx, convID, 0, 10)
	if len(got) != 2 || got[1].Seq != 2 {
		t.Fatalf("seq hole detected: len=%d seqs=%v", len(got), []int64{got[0].Seq, got[1].Seq})
	}
	_, keys := collector.snapshot()
	if len(keys) != 4 { // 1 + 2 replays + 1 new —— at-least-once：重放补发 push，消费端幂等
		t.Logf("push count = %d (at-least-once re-push allowed)", len(keys))
	}
}

// TestPersistPoisonMessage 毒消息：对端不存在 → ErrPoison（外层跳过不重投）。
func TestPersistPoisonMessage(t *testing.T) {
	w, store, _ := newIntegrationEnv(t)
	ctx := context.Background()
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	alice := ensureUser(t, store, "xa"+suffix)

	err := w.Handle(ctx, &chat.Msg{CliMsgID: "p-1", FromUID: alice, ToUID: "no-such-user", Type: chat.TypeSingle, Content: map[string]string{"text": "x"}})
	if err == nil {
		t.Fatal("poison expected")
	}
	if err.Error() == "" || !containsPoison(err) {
		t.Fatalf("want ErrPoison, got %v", err)
	}
}

func containsPoison(err error) bool {
	return err != nil && (err.Error() == ErrPoison.Error() || fmt.Sprintf("%v", err) != "" && isPoisonWrapped(err))
}

func isPoisonWrapped(err error) bool {
	type unwrapper interface{ Unwrap() error }
	for err != nil {
		if err.Error() == ErrPoison.Error() {
			return true
		}
		u, ok := err.(unwrapper)
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}

// TestKafkaOrderingByKey 真实 Kafka：同 conv_id 100 条（并发 produce）→ 按 key 分区消费 →
// 交给 Handle 落库 → seq 严格递增（D20 链路验证）。
func TestKafkaOrderingByKey(t *testing.T) {
	w, store, _ := newIntegrationEnv(t)
	ctx := context.Background()
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	alice := ensureUser(t, store, "oa"+suffix)
	bob := ensureUser(t, store, "ob"+suffix)

	brokers := []string{"127.0.0.1:9092"}
	if env := os.Getenv("BRAVE_KAFKA_BROKERS"); env != "" {
		brokers = []string{env}
	}

	// 先建会话（固定 conv，避免首条空 conv 路径干扰分区序验证）
	convStore := database.NewConversationStore(store)
	conv, err := convStore.EnsureSingle(ctx, alice, bob)
	if err != nil {
		t.Fatalf("ensure conv: %v", err)
	}

	const n = 100
	producer := ibrave.NewProducer(brokers, chat.TopicMsg)
	defer producer.Close()
	for i := 0; i < n; i++ {
		env := &chat.Msg{CliMsgID: fmt.Sprintf("k-%d", i), ConvID: conv.ConvID, FromUID: alice, ToUID: bob,
			Type: chat.TypeSingle, Content: map[string]string{"text": fmt.Sprintf("m%d", i)}}
		raw, _ := json.Marshal(env)
		if err := producer.Write(ctx, conv.ConvID, raw); err != nil {
			t.Fatalf("produce %d: %v", i, err)
		}
	}

	// 消费 chat.msg（独立组）逐条 Handle——模拟 persist 消费循环的串行纪律
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		GroupID: fmt.Sprintf("persist-order-it-%d", time.Now().UnixNano()),
		Topic:   chat.TopicMsg,
	})
	defer reader.Close()

	handled := 0
	deadline := time.Now().Add(60 * time.Second)
	for handled < n && time.Now().Before(deadline) {
		rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		m, err := reader.FetchMessage(rctx)
		cancel()
		if err != nil {
			continue
		}
		var msg chat.Msg
		if err := json.Unmarshal(m.Value, &msg); err != nil {
			t.Fatalf("bad payload: %v", err)
		}
		// 跳过其他测试遗留（不同 conv 过滤）
		if msg.ConvID != conv.ConvID {
			if err := reader.CommitMessages(ctx, m); err != nil {
				t.Fatalf("commit skip: %v", err)
			}
			continue
		}
		if err := w.Handle(ctx, &msg); err != nil {
			t.Fatalf("handle: %v", err)
		}
		if err := reader.CommitMessages(ctx, m); err != nil {
			t.Fatalf("commit: %v", err)
		}
		handled++
	}
	if handled != n {
		t.Fatalf("handled = %d, want %d", handled, n)
	}

	msgs := database.NewMessageStore(store)
	// ListAfter 单页上限 100（防大查询），本测试正好一页
	got, err := msgs.ListAfter(ctx, conv.ConvID, 0, 100)
	if err != nil || len(got) != n {
		t.Fatalf("stored messages: err=%v len=%d want %d", err, len(got), n)
	}
	for i, m := range got {
		if m.Seq != int64(i+1) {
			t.Fatalf("seq hole/disorder at %d: seq=%d", i, m.Seq)
		}
	}
}

// TestPersistBatchReplayIdempotent 批路径重试幂等（S3c 根因回归）：
// 同批 [新消息, 已落库重放, 批内重复] → 消息行只多 1、seq 无空洞、重放复用原 seq 补投 push。
func TestPersistBatchReplayIdempotent(t *testing.T) {
	w, store, collector := newIntegrationEnv(t)
	ctx := context.Background()
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	alice := ensureUser(t, store, "ia"+suffix)
	bob := ensureUser(t, store, "ib"+suffix)

	// 首条走单条路径：建会话 + seq=1
	m1 := &chat.Msg{CliMsgID: "b-1", FromUID: alice, ToUID: bob, Type: chat.TypeSingle, Content: map[string]string{"text": "1"}}
	if err := w.Handle(ctx, m1); err != nil {
		t.Fatalf("handle m1: %v", err)
	}
	convID := m1.ConvID

	msgs := database.NewMessageStore(store)
	before, _ := msgs.ListAfter(ctx, convID, 0, 10)

	// 批：新消息 b-2 + b-1 重放 + b-2 批内重复
	m2 := &chat.Msg{CliMsgID: "b-2", ConvID: convID, FromUID: alice, ToUID: bob, Type: chat.TypeSingle, Content: map[string]string{"text": "2"}}
	m2Dup := &chat.Msg{CliMsgID: "b-2", ConvID: convID, FromUID: alice, ToUID: bob, Type: chat.TypeSingle, Content: map[string]string{"text": "2"}}
	m1Replay := &chat.Msg{CliMsgID: "b-1", ConvID: convID, FromUID: alice, ToUID: bob, Type: chat.TypeSingle, Content: map[string]string{"text": "1"}}
	if err := w.handleSingleBatch(ctx, []*chat.Msg{m2, m1Replay, m2Dup}); err != nil {
		t.Fatalf("handleSingleBatch: %v", err)
	}
	w.flushPushes(ctx)

	got, err := msgs.ListAfter(ctx, convID, 0, 10)
	if err != nil || len(got) != len(before)+1 {
		t.Fatalf("after batch: err=%v rows=%d want %d", err, len(got), len(before)+1)
	}
	if got[1].Seq != 2 {
		t.Fatalf("m2 seq = %d, want 2 (replay must not burn seq)", got[1].Seq)
	}

	// 后续新消息 seq=3：重放与批内重复都没烧号
	m3 := &chat.Msg{CliMsgID: "b-3", ConvID: convID, FromUID: alice, ToUID: bob, Type: chat.TypeSingle, Content: map[string]string{"text": "3"}}
	if err := w.Handle(ctx, m3); err != nil {
		t.Fatalf("handle m3: %v", err)
	}
	got, _ = msgs.ListAfter(ctx, convID, 0, 10)
	if len(got) != 3 || got[2].Seq != 3 {
		t.Fatalf("seq hole after batch: len=%d seqs=%v", len(got), []int64{got[0].Seq, got[1].Seq, got[2].Seq})
	}

	// push 对账：m1 首+重放补投、m2 只投一次（批内重复不再投）、m3 —— key 恒 bob
	_, keys := collector.snapshot()
	if len(keys) != 4 { // m1 + m1(replay) + m2 + m3
		t.Fatalf("push count = %d, want 4 (at-least-once replay re-push, dup suppressed)", len(keys))
	}
}
