package deliver

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"alfred.brave.com/database"
)

// fakeKv 可编程 kv：data 可变，模拟在线状态变化/路由漂移。
type fakeKv struct {
	data map[string]string
}

func (f *fakeKv) Get(ctx context.Context, key string, out interface{}) error {
	raw, ok := f.data[key]
	if !ok {
		return database.ErrNotFound
	}
	return json.Unmarshal([]byte(raw), out)
}

// TestRouterCacheHitTTL 缓存命中不回源；过期回源拿到新值（TTL 上界语义）。
func TestRouterCacheHitTTL(t *testing.T) {
	now := time.Now()
	kv := &fakeKv{data: map[string]string{
		"online:u1": `{"cs":"cs-1","addr":"a1"}`,
	}}
	r := NewRouter(kv)
	r.nowFn = func() time.Time { return now }

	got, err := r.Get(context.Background(), "u1")
	if err != nil || got.Addr != "a1" {
		t.Fatalf("first get: %v %+v", err, got)
	}

	// 权威值变了，但缓存未过期：仍读旧值（TTL 上界内的陈旧是被接受的代价，D06）
	kv.data["online:u1"] = `{"cs":"cs-2","addr":"a2"}`
	got, err = r.Get(context.Background(), "u1")
	if err != nil || got.Addr != "a1" {
		t.Fatalf("cached get must serve stale within TTL: %v %+v", err, got)
	}

	// 时间越过 TTL：回源拿新值
	now = now.Add(RouteTTL + time.Second)
	got, err = r.Get(context.Background(), "u1")
	if err != nil || got.Addr != "a2" {
		t.Fatalf("expired get must refetch: %v %+v", err, got)
	}
}

// TestRouterOfflineNegativeCache 离线负缓存：不存在也缓存（空 addr），TTL 内不重复打 PG。
func TestRouterOfflineNegativeCache(t *testing.T) {
	now := time.Now()
	kv := &fakeKv{data: map[string]string{}}
	r := NewRouter(kv)
	r.nowFn = func() time.Time { return now }

	got, err := r.Get(context.Background(), "ghost")
	if err != nil || got.Addr != "" {
		t.Fatalf("offline: %v %+v", err, got)
	}
	// 上线（权威有值）但负缓存未过期：仍离线视图
	kv.data["online:ghost"] = `{"cs":"cs-9","addr":"a9"}`
	got, _ = r.Get(context.Background(), "ghost")
	if got.Addr != "" {
		t.Fatal("negative cache must hold until TTL")
	}
	// Invalidate（防线②）后立即见新值
	r.Invalidate("ghost")
	got, err = r.Get(context.Background(), "ghost")
	if err != nil || got.Addr != "a9" {
		t.Fatalf("after invalidate: %v %+v", err, got)
	}
}
