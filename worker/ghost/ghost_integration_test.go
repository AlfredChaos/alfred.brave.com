//go:build integration

// 集成测试：真实 PG 上的 online upsert → 对账清理闭环。
package ghost

import (
	"context"
	"os"
	"testing"
	"time"

	"alfred.brave.com/database"
	"alfred.brave.com/joker/exchange"
)

func newGhostKv(t *testing.T) *database.KvStore {
	t.Helper()
	dsn := os.Getenv("BRAVE_PG_DSN")
	if dsn == "" {
		dsn = "postgres://brave:brave@127.0.0.1:55432/brave?sslmode=disable"
	}
	store, err := database.NewStore(context.Background(), dsn, 4)
	if err != nil {
		t.Fatalf("connect pg: %v", err)
	}
	t.Cleanup(store.Close)
	return database.NewKvStore(store)
}

// TestGhostSweepIntegration online upsert → 存活判定 → GHOST 清理 → 漂移保护 闭环。
func TestGhostSweepIntegration(t *testing.T) {
	kv := newGhostKv(t)
	ctx := context.Background()
	online := exchange.NewPgOnlineKV(kv)

	uidLive := "ghost-it-live-" + time.Now().Format("150405.000000000")
	uidDead := "ghost-it-dead-" + time.Now().Format("150405.000000000")

	// live 记录归属存活 CS；dead 记录归属已下线 CS
	if err := online.Upsert(ctx, uidLive, "cs-alive:37002", "cs-alive:37012"); err != nil {
		t.Fatalf("upsert live: %v", err)
	}
	if err := online.Upsert(ctx, uidDead, "cs-dead:37002", "cs-dead:37012"); err != nil {
		t.Fatalf("upsert dead: %v", err)
	}

	r := NewReconciler(kv, func() map[string]bool {
		return map[string]bool{"cs-alive:37002": true}
	})
	removed, err := r.Sweep(ctx)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if removed != 1 {
		t.Fatalf("removed = %d, want 1", removed)
	}

	var v exchange.OnlineValue
	if err := kv.Get(ctx, "online:"+uidLive, &v); err != nil || v.Cs != "cs-alive:37002" {
		t.Fatalf("live record must survive: %v %+v", err, v)
	}
	if err := kv.Get(ctx, "online:"+uidDead, &v); err != database.ErrNotFound {
		t.Fatalf("dead record must be removed, got %v", err)
	}

	// 漂移保护：live 记录改归属 cs-alive 后，cs-dead 的晚到 del 不生效
	if err := online.Upsert(ctx, uidLive, "cs-new:37002", "cs-new:37012"); err != nil {
		t.Fatalf("upsert drift: %v", err)
	}
	if err := online.DeleteIfMatch(ctx, uidLive, "cs-dead:37002"); err != nil {
		t.Fatalf("deleteIfMatch: %v", err)
	}
	if err := kv.Get(ctx, "online:"+uidLive, &v); err != nil || v.Cs != "cs-new:37002" {
		t.Fatalf("stale delete must not remove drifted record: %v %+v", err, v)
	}

	// 清理测试数据
	if err := kv.Delete(ctx, "online:"+uidLive); err != nil {
		t.Fatalf("cleanup: %v", err)
	}
}
