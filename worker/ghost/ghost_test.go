package ghost

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"alfred.brave.com/database"
)

// fakeKV GHOST 对账单测用假 kv：模拟真实 PG 后端的 key 游标语义
// （afterKey 过滤 + 有序 + 按调用方 limit 截断）。
type fakeKV struct {
	entries map[string]database.KvEntry
	deleted []string
}

func (f *fakeKV) ScanPrefix(ctx context.Context, prefix, afterKey string, limit int) ([]database.KvEntry, error) {
	keys := make([]string, 0, len(f.entries))
	for k := range f.entries {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix && k > afterKey {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	if limit > 0 && len(keys) > limit {
		keys = keys[:limit]
	}
	out := make([]database.KvEntry, 0, len(keys))
	for _, k := range keys {
		out = append(out, f.entries[k])
	}
	return out, nil
}

func (f *fakeKV) DeleteIfMatch(ctx context.Context, key, field, self string) error {
	f.deleted = append(f.deleted, key)
	delete(f.entries, key)
	return nil
}

func newFake(t *testing.T) *fakeKV {
	t.Helper()
	entries := map[string]database.KvEntry{
		"online:alive-user": {Key: "online:alive-user", Value: []byte(`{"cs":"cs-1:37002","addr":"cs-1:37012"}`)},
		"online:ghost-user": {Key: "online:ghost-user", Value: []byte(`{"cs":"cs-dead:37002","addr":"cs-dead:37012"}`)},
	}
	return &fakeKV{entries: entries}
}

// TestSweepRemovesGhost 对账清理：存活 CS 的记录保留，死 CS 的记录删除。
func TestSweepRemovesGhost(t *testing.T) {
	fkv := newFake(t)
	r := NewReconciler(fkv, func() map[string]bool {
		return map[string]bool{"cs-1:37002": true}
	})

	removed, err := r.Sweep(context.Background())
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if removed != 1 {
		t.Fatalf("removed = %d, want 1", removed)
	}
	if _, ok := fkv.entries["online:ghost-user"]; ok {
		t.Fatal("ghost record must be removed")
	}
	if _, ok := fkv.entries["online:alive-user"]; !ok {
		t.Fatal("alive record must survive")
	}
}

// TestSweepSkipsBadPayload 坏 JSON 记录跳过不删（告警），不影响其余清理。
func TestSweepSkipsBadPayload(t *testing.T) {
	fkv := newFake(t)
	fkv.entries["online:bad-json"] = database.KvEntry{Key: "online:bad-json", Value: []byte(`{not-json`)}
	r := NewReconciler(fkv, func() map[string]bool { return map[string]bool{} })

	removed, err := r.Sweep(context.Background())
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	// 存活表为空：两条可解析记录都清，坏 JSON 只跳过——removed = 2 而非 3
	if removed != 2 {
		t.Fatalf("removed = %d, want 2 (bad json skipped)", removed)
	}
	if _, ok := fkv.entries["online:bad-json"]; !ok {
		t.Fatal("bad-json record must be kept (skipped, not deleted)")
	}
}

// TestSweepEmptyAliveTable 全部 CS 下线（如 etcd 抖动）：对账保守起见只清理解析成功的记录——
// 现实中 alive 表为空意味着 etcd 视图异常，本实现仍按视图执行（lease 60s 保证下线节点不再注册）。
func TestSweepEmptyAliveTable(t *testing.T) {
	fkv := newFake(t)
	r := NewReconciler(fkv, func() map[string]bool { return map[string]bool{} })

	removed, err := r.Sweep(context.Background())
	if err != nil || removed != 2 {
		t.Fatalf("removed = %d err = %v, want 2 nil", removed, err)
	}
}

// TestSweepPaginatesLargeTable 分页回归：1200 条记录、fake 每页只回 500 ——
// 旧实现单页上限即全表上限，75 万在线压测场景会漏掉 99% 残留（实测前修复）。
func TestSweepPaginatesLargeTable(t *testing.T) {
	fkv := &fakeKV{entries: map[string]database.KvEntry{}}
	for i := 0; i < 1200; i++ {
		k := "online:u" + fmt.Sprintf("%06d", i)
		fkv.entries[k] = database.KvEntry{Key: k, Value: []byte(`{"cs":"cs-dead:37002","addr":"cs-dead:37012"}`)}
	}
	r := NewReconciler(fkv, func() map[string]bool { return map[string]bool{} })
	r.pageSize = 500 // 压小页触发 1200/500 = 3 页路径

	removed, err := r.Sweep(context.Background())
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if removed != 1200 || len(fkv.entries) != 0 {
		t.Fatalf("removed=%d left=%d, want 1200/0 (pagination must cover full table)", removed, len(fkv.entries))
	}
}

// TestRunRespectsContext Run 循环随 context 取消退出。
func TestRunRespectsContext(t *testing.T) {
	fkv := newFake(t)
	r := NewReconciler(fkv, func() map[string]bool { return map[string]bool{} })
	r.interval = 10 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.Run(ctx); close(done) }()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not exit after cancel")
	}
}
