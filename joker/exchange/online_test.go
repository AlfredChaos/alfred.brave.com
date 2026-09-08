package exchange

import (
	"context"
	"encoding/json"
	"testing"

	"alfred.brave.com/database"
	"alfred.brave.com/internal/chat"
	"alfred.brave.com/joker/proto"
)

// fakeOnlineKV 记录调用序列的假 kv，用于验证状态迁移触发的读写。
type fakeOnlineKV struct {
	upserts  []string // "uid|cs"
	dels     []string // "uid|cs"
	delCalls int
	lookup   map[string]OnlineValue
}

func (f *fakeOnlineKV) Upsert(ctx context.Context, uid, cs, addr string) error {
	f.upserts = append(f.upserts, uid+"|"+cs)
	return nil
}

func (f *fakeOnlineKV) DeleteIfMatch(ctx context.Context, uid, cs string) error {
	f.delCalls++
	f.dels = append(f.dels, uid+"|"+cs)
	return nil
}

func (f *fakeOnlineKV) GetMany(ctx context.Context, uids []string) (map[string]OnlineValue, error) {
	out := make(map[string]OnlineValue, len(uids))
	if f.lookup == nil {
		return out, nil
	}
	for _, uid := range uids {
		if v, ok := f.lookup[uid]; ok {
			out[uid] = v
		}
	}
	return out, nil
}

// TestOnlineUpsertOnRegister 连接建立必须 upsert online:{uid}，cs 为本机服务地址。
func TestOnlineUpsertOnRegister(t *testing.T) {
	fake := &fakeOnlineKV{}
	m := NewManager()
	m.SetOnline(fake)
	m.SetOnlineIdentity("cs-1:37002", "cs-1:37012")

	c := NewClient(m, "u1", nil)
	m.EventRegister(c)

	if len(fake.upserts) != 1 || fake.upserts[0] != "u1|cs-1:37002" {
		t.Fatalf("upserts = %v, want [u1|cs-1:37002]", fake.upserts)
	}
}

// TestOnlineDeleteOnUnregister 断开必须条件删除（带自身 cs，防漂移误删）。
func TestOnlineDeleteOnUnregister(t *testing.T) {
	fake := &fakeOnlineKV{}
	m := NewManager()
	m.SetOnline(fake)
	m.SetOnlineIdentity("cs-1:37002", "cs-1:37012")

	c := NewClient(m, "u2", nil)
	m.EventRegister(c)
	m.EventUnregister(c)

	if len(fake.dels) != 1 || fake.dels[0] != "u2|cs-1:37002" {
		t.Fatalf("dels = %v, want [u2|cs-1:37002]", fake.dels)
	}
}

// TestOnlineNilKeepsLocalMode 未注入 online 依赖时（本地裸跑/测试），连接表照常工作。
func TestOnlineNilKeepsLocalMode(t *testing.T) {
	m := NewManager()
	c := NewClient(m, "u3", nil)
	m.EventRegister(c)
	m.EventUnregister(c) // 不得 panic
	if m.GetClient("u3") != nil {
		t.Fatal("client should be unregistered")
	}
}

var _ = database.ErrNotFound // 保持 database 引用（OnlineKV 默认实现所在包）

type fakeAckRelayer struct {
	calls []string
}

func (f *fakeAckRelayer) BatchRelayAcks(ctx context.Context, addr string, req *jokerproto.BatchRelayAcksRequest) (*jokerproto.BatchRelayAcksResponse, error) {
	f.calls = append(f.calls, addr)
	out := &jokerproto.BatchRelayAcksResponse{Results: make([]*jokerproto.RelayMessageResponse, 0, len(req.Acks))}
	for range req.Acks {
		out.Results = append(out.Results, &jokerproto.RelayMessageResponse{Delivered: true})
	}
	return out, nil
}

func TestAckDispatchLocalAndRemote(t *testing.T) {
	m := NewManager()
	m.SetOnlineIdentity("cs-local:37002", "cs-local:37012")
	fake := &fakeOnlineKV{lookup: map[string]OnlineValue{
		"local-user":  {Addr: "cs-local:37012"},
		"remote-user": {Addr: "cs-remote:37012"},
	}}
	m.SetOnline(fake)
	local := NewClient(m, "local-user", nil)
	m.EventRegister(local)
	relayer := &fakeAckRelayer{}
	d := &ackDispatcher{manager: m, relayer: relayer}

	err := d.dispatch(context.Background(), []chat.Ack{
		{MsgID: "m-local", CliMsgID: "c-local", FromUID: "local-user"},
		{MsgID: "m-remote", CliMsgID: "c-remote", FromUID: "remote-user"},
		{MsgID: "m-offline", CliMsgID: "c-off", FromUID: "offline-user"},
	})
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	select {
	case raw := <-local.Send:
		var frame AckFrame
		if err := json.Unmarshal(raw, &frame); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if frame.Cmd != "ack" || frame.Data.MsgID != "m-local" {
			t.Fatalf("local ack mismatch: %+v", frame)
		}
	default:
		t.Fatal("local sender did not receive ack")
	}
	if len(relayer.calls) != 1 || relayer.calls[0] != "cs-remote:37012" {
		t.Fatalf("remote calls = %v, want [cs-remote:37012]", relayer.calls)
	}
}
