//go:build integration

// 集成测试：群聊投递（§5 步骤 4-9）——权威校验 + 仅在线成员扇出 + 顺序域 gid→uid 切换。
package persist

import (
	"context"
	"fmt"
	"testing"
	"time"

	"alfred.brave.com/database"
	"alfred.brave.com/internal/chat"
)

type groupFixture struct {
	w       *Worker
	store   *database.Store
	gid     string
	owner   string
	m1      string // 在线成员
	m2      string // 离线成员
	outside string
}

func newGroupFixture(t *testing.T) *groupFixture {
	t.Helper()
	_, store, collector := newIntegrationEnv(t)
	ctx := context.Background()

	f := &groupFixture{w: nil, store: store}
	f.w = &Worker{
		convs:  database.NewConversationStore(store),
		users:  database.NewUserStore(store),
		groups: database.NewGroupStore(store),
		kv:     database.NewKvStore(store),
		pool:   store,
		push:   collector,
	}
	f.owner = ensureUser(t, store, "go"+fmt.Sprint(time.Now().UnixNano()))
	f.m1 = ensureUser(t, store, "gm1"+fmt.Sprint(time.Now().UnixNano()))
	f.m2 = ensureUser(t, store, "gm2"+fmt.Sprint(time.Now().UnixNano()))
	f.outside = ensureUser(t, store, "gx"+fmt.Sprint(time.Now().UnixNano()))

	// 建群（owner + m1 + m2）
	gen := func() (string, error) { return "7" + fmt.Sprint(time.Now().UnixNano()), nil }
	gid, _, err := database.NewGroupStore(store).CreateGroup(ctx, gen, f.owner, "test", []string{f.m1, f.m2})
	if err != nil {
		t.Fatalf("create group: %v", err)
	}
	f.gid = gid

	// m1 在线（写 online kv）；m2 离线（不写）
	kv := database.NewKvStore(store)
	if err := kv.Put(ctx, "online:"+f.m1, map[string]string{"cs": "cs-x", "addr": "cs-x:37012"}); err != nil {
		t.Fatalf("kv put online: %v", err)
	}
	return f
}

func (f *groupFixture) collector() *fakePushCollector { return f.w.push.(*fakePushCollector) }

// TestGroupFanoutOnlineOnly 群消息 → 存储恒 1 份 → 仅在线成员（m1）收到 chat.push（key=m1）。
func TestGroupFanoutOnlineOnly(t *testing.T) {
	f := newGroupFixture(t)
	ctx := context.Background()

	msg := &chat.Msg{
		ConvID: f.gid, FromUID: f.owner, Type: chat.TypeGroup,
		CliMsgID: "g-1", Content: map[string]string{"text": "hello group"},
	}
	if err := f.w.Handle(ctx, msg); err != nil {
		t.Fatalf("handle: %v", err)
	}

	// 存储一份
	msgs := database.NewMessageStore(f.store)
	list, err := msgs.ListAfter(ctx, f.gid, 0, 10)
	if err != nil || len(list) != 2 { // seq=1 是建群 system_event，seq=2 是本条
		t.Fatalf("stored messages: err=%v len=%d (want 2: create-event + msg)", err, len(list))
	}
	if list[1].Seq != 2 || list[1].Type != chat.TypeGroup {
		t.Fatalf("group msg at seq=2 expected: %+v", list[1])
	}

	// 扇出：只有在线的 m1
	pushes, keys := f.collector().snapshot()
	if len(pushes) != 1 {
		t.Fatalf("pushes = %d, want 1 (online member only)", len(pushes))
	}
	if pushes[0].ToUID != f.m1 || keys[0] != f.m1 {
		t.Fatalf("push target = %v, want online member %s", keys, f.m1)
	}
	if pushes[0].Seq != 2 || pushes[0].Type != chat.TypeGroup {
		t.Fatalf("push payload: %+v", pushes[0])
	}
}

// TestGroupRejectedSenders 解散群 / 非成员发送 → ErrPoison 拒收（§5 persist 权威校验）。
func TestGroupRejectedSenders(t *testing.T) {
	f := newGroupFixture(t)
	ctx := context.Background()

	// 非成员发送
	err := f.w.Handle(ctx, &chat.Msg{ConvID: f.gid, FromUID: f.outside, Type: chat.TypeGroup, Content: map[string]string{"text": "x"}})
	if !IsPoison(err) {
		t.Fatalf("non-member sender must be poison, got %v", err)
	}
	// 未知群
	err = f.w.Handle(ctx, &chat.Msg{ConvID: "9999999999999", FromUID: f.owner, Type: chat.TypeGroup, Content: map[string]string{"text": "x"}})
	if !IsPoison(err) {
		t.Fatalf("unknown group must be poison, got %v", err)
	}

	// 解散后再发 → 拒收（§5：发完 dismiss 事件后 persist 拒收该 gid 一切后续消息）
	if _, err := database.NewGroupStore(f.store).Dismiss(ctx, f.gid, f.owner); err != nil {
		t.Fatalf("dismiss: %v", err)
	}
	err = f.w.Handle(ctx, &chat.Msg{ConvID: f.gid, FromUID: f.owner, Type: chat.TypeGroup, CliMsgID: "after", Content: map[string]string{"text": "x"}})
	if !IsPoison(err) {
		t.Fatalf("dismissed group must be poison, got %v", err)
	}
	// 解散事件本身的信封投递不受影响（预写消息幂等扇出）
}

// TestGroupSystemEventEnvelopeFanout 群事件信封（预写 msg_id）→ 跳过落库 → 在线成员扇出。
func TestGroupSystemEventEnvelopeFanout(t *testing.T) {
	f := newGroupFixture(t)
	ctx := context.Background()

	// 网关路径：管理操作预写 system_event 后投递信封
	gs := database.NewGroupStore(f.store)
	msgID, err := gs.Rename(ctx, f.gid, f.owner, "renamed")
	if err != nil {
		t.Fatalf("rename: %v", err)
	}
	before, _ := database.NewMessageStore(f.store).ListAfter(ctx, f.gid, 0, 100)

	env := &chat.Msg{MsgID: msgID, ConvID: f.gid, FromUID: f.owner, Type: chat.TypeSystemEvent, Content: map[string]interface{}{}}
	if err := f.w.Handle(ctx, env); err != nil {
		t.Fatalf("handle envelope: %v", err)
	}

	after, _ := database.NewMessageStore(f.store).ListAfter(ctx, f.gid, 0, 100)
	if len(after) != len(before) {
		t.Fatalf("envelope must not insert duplicate row: before=%d after=%d", len(before), len(after))
	}
	pushes, _ := f.collector().snapshot()
	if len(pushes) == 0 {
		t.Fatal("envelope must fan out to online members")
	}
	found := false
	for _, p := range pushes {
		if p.MsgID == msgID && p.ToUID == f.m1 && p.Type == chat.TypeSystemEvent {
			found = true
		}
	}
	if !found {
		t.Fatalf("system_event push to online member missing: %+v", pushes)
	}
}

// TestGroupOrderingSameGid 同 gid 多条消息 → seq 严格递增（chat.msg key=gid 分区序 + persist 串行）。
func TestGroupOrderingSameGid(t *testing.T) {
	f := newGroupFixture(t)
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		msg := &chat.Msg{
			ConvID: f.gid, FromUID: f.owner, Type: chat.TypeGroup,
			CliMsgID: fmt.Sprintf("ord-%d", i), Content: map[string]string{"text": "m"},
		}
		if err := f.w.Handle(ctx, msg); err != nil {
			t.Fatalf("handle %d: %v", i, err)
		}
	}
	msgs, _ := database.NewMessageStore(f.store).ListAfter(ctx, f.gid, 0, 100)
	// seq=1 建群事件 + 10 条 = 11，严格 1..11 无空洞
	if len(msgs) != 11 {
		t.Fatalf("messages = %d, want 11", len(msgs))
	}
	for i, m := range msgs {
		if m.Seq != int64(i+1) {
			t.Fatalf("seq hole/disorder at %d: seq=%d", i, m.Seq)
		}
	}
}
