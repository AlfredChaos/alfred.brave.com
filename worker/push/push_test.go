package push

import (
	"context"
	"sync"
	"testing"
	"time"

	"alfred.brave.com/internal/chat"
)

// fakeVendor 记录推送调用。
type fakeVendor struct {
	mu    sync.Mutex
	calls []string
}

func (f *fakeVendor) Push(ctx context.Context, token, title, body string, badge int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, token)
	return nil
}

func (f *fakeVendor) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

// TestPushMergeWindow 同人 60s 合并：窗口首条立即发；窗口内后续只累加 badge；
// 到期 flush 合并播报一次（badge=4）。
func TestPushMergeWindow(t *testing.T) {
	vendor := &fakeVendor{}
	w := New(vendor, []string{"127.0.0.1:9092"}, "push-it")
	base := time.Now()
	w.nowFn = func() time.Time { return base }

	ev := chat.Notify{ToUID: "u1", FromUID: "u2", ConvID: "c", Seq: 1}
	w.ingest(context.Background(), ev)
	if vendor.count() != 1 {
		t.Fatalf("first event must send immediately, sent=%d", vendor.count())
	}

	// 窗口内 3 条：不发送（badge 累加到 4）
	for i := 0; i < 3; i++ {
		w.ingest(context.Background(), ev)
	}
	if vendor.count() != 1 {
		t.Fatalf("in-window events must merge, sent=%d", vendor.count())
	}

	// 窗口到期 flush：合并 badge 重发一次
	w.nowFn = func() time.Time { return base.Add(61 * time.Second) }
	w.flushDue(context.Background())
	if vendor.count() != 2 {
		t.Fatalf("merged flush expected, sent=%d", vendor.count())
	}
}

// TestPushHourlyLimit 单设备 ≤20/h：超限只保 badge（丢弃发送，日志留痕）。
func TestPushHourlyLimit(t *testing.T) {
	vendor := &fakeVendor{}
	w := New(vendor, []string{"127.0.0.1:9092"}, "push-it")
	base := time.Now().Truncate(time.Hour)
	now := base
	w.nowFn = func() time.Time { return now }

	// 21 个不同窗口（每个窗口一条，避免合并），一小时内的第 21 条被频控
	for i := 0; i < deviceHourly+1; i++ {
		now = base.Add(time.Duration(i) * (mergeWindow + time.Second))
		w.ingest(context.Background(), chat.Notify{ToUID: "u9", Seq: int64(i)})
	}
	if got := vendor.count(); got != deviceHourly {
		t.Fatalf("hourly limit: sent=%d, want %d", got, deviceHourly)
	}
}

// TestPushDifferentUsersDontMerge 不同用户独立窗口互不合并。
func TestPushDifferentUsersDontMerge(t *testing.T) {
	vendor := &fakeVendor{}
	w := New(vendor, []string{"127.0.0.1:9092"}, "push-it")
	base := time.Now()
	w.nowFn = func() time.Time { return base }

	w.ingest(context.Background(), chat.Notify{ToUID: "a", Seq: 1})
	w.ingest(context.Background(), chat.Notify{ToUID: "b", Seq: 1})
	if vendor.count() != 2 {
		t.Fatalf("independent users must not merge, sent=%d", vendor.count())
	}
}
