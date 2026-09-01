package exchange

import (
	"context"
	"testing"

	"alfred.brave.com/database"
)

// fakeOnlineKV 记录调用序列的假 kv，用于验证状态迁移触发的读写。
type fakeOnlineKV struct {
	upserts  []string // "uid|cs"
	dels     []string // "uid|cs"
	delCalls int
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
