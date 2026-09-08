// deliver worker 故障路径单测（不依赖 PG/Kafka，relay/kv 全部接口注入）：
// 覆盖 CS 宕机三要素——同步重试、per-call 超时、失效连接驱逐。
package deliver

import (
	"context"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"alfred.brave.com/internal/chat"
	"alfred.brave.com/joker/exchange"
	"alfred.brave.com/joker/proto"
	"alfred.brave.com/joker/relay"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fakeRelayer 可编程 relay：按调用序号返回结果，记录 ctx 供超时断言。
// fn 可被测试中途改写（模拟故障恢复），读写均在锁内。
type fakeRelayer struct {
	mu    sync.Mutex
	calls int
	fn    func(ctx context.Context, call int) (*jokerproto.RelayMessageResponse, error)
}

func (f *fakeRelayer) Relay(ctx context.Context, addr string, req *jokerproto.RelayMessageRequest) (*jokerproto.RelayMessageResponse, error) {
	f.mu.Lock()
	f.calls++
	n := f.calls
	fn := f.fn
	f.mu.Unlock()
	return fn(ctx, n)
}

func (f *fakeRelayer) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// newTestWorker 绕过 New（避免真实 kafka reader）：kv 注入 online 记录指向 fake addr。
func newTestWorker(kv *fakeKv, relayer Relayer) *Worker {
	return &Worker{
		router:      NewRouter(kv),
		kv:          nil,
		relayer:     relayer,
		onDelivered: func(context.Context, *chat.Push) {},
		onOffline:   func(context.Context, *chat.Push) {},
	}
}

func onlineKv(addr string) *fakeKv {
	return &fakeKv{data: map[string]string{
		"online:u1": `{"cs":"cs-1","addr":"` + addr + `"}`,
	}}
}

func testPush() *chat.Push {
	return &chat.Push{MsgID: "m-1", ConvID: "c", Seq: 1, FromUID: "a", ToUID: "u1", Type: chat.TypeSingle, Content: map[string]string{"text": "x"}}
}

// TestDeliverWithRetryTransientFail 前两次连接级失败、第三次成功：重试穿越瞬时故障。
// 重试前会 Invalidate 路由缓存，故 kv 中地址可中途漂移，第三次必须投到新地址。
func TestDeliverWithRetryTransientFail(t *testing.T) {
	kv := onlineKv("dead:1")
	relayer := &fakeRelayer{fn: func(_ context.Context, call int) (*jokerproto.RelayMessageResponse, error) {
		if call < 3 {
			return nil, status.Error(codes.Unavailable, "cs down")
		}
		return &jokerproto.RelayMessageResponse{Delivered: true}, nil
	}}
	w := newTestWorker(kv, relayer)

	if err := w.deliverWithRetry(context.Background(), testPush()); err != nil {
		t.Fatalf("transient failure must be retried through, got %v", err)
	}
	if got := relayer.count(); got != 3 {
		t.Fatalf("relay calls = %d, want 3", got)
	}
}

// TestDeliverWithRetryAllFail 持续失败：重试满次数后如实返回错误（Run 层据此跳过）。
func TestDeliverWithRetryAllFail(t *testing.T) {
	w := newTestWorker(onlineKv("dead:1"), &fakeRelayer{fn: func(context.Context, int) (*jokerproto.RelayMessageResponse, error) {
		return nil, status.Error(codes.Unavailable, "cs down")
	}})

	err := w.deliverWithRetry(context.Background(), testPush())
	if err == nil || status.Code(err) != codes.Unavailable {
		t.Fatalf("persistent failure must surface, got %v", err)
	}
}

// TestDeliverRetryPicksUpDriftedRoute CS 宕机后用户重连漂移：重试轮次间 kv 已指向新
// 地址（fakeRelayer 按 addr 区分成败），Invalidate 后的重试必须拿到新地址投递成功。
func TestDeliverRetryPicksUpDriftedRoute(t *testing.T) {
	kv := onlineKv("dead:1")
	relayer := &fakeRelayer{fn: func(_ context.Context, _ int) (*jokerproto.RelayMessageResponse, error) {
		return nil, status.Error(codes.Unavailable, "cs down")
	}}
	// 首投失败后权威值漂移到新节点（模拟用户重连 upsert）
	w := newTestWorker(kv, relayer)
	go func() {
		time.Sleep(200 * time.Millisecond)
		kv.data["online:u1"] = `{"cs":"cs-2","addr":"alive:2"}`
		relayer.mu.Lock()
		relayer.fn = func(_ context.Context, _ int) (*jokerproto.RelayMessageResponse, error) {
			return &jokerproto.RelayMessageResponse{Delivered: true}, nil
		}
		relayer.mu.Unlock()
	}()

	if err := w.deliverWithRetry(context.Background(), testPush()); err != nil {
		t.Fatalf("retry must pick up drifted route, got %v", err)
	}
}

// TestDeliverRelayTimeout 对端挂起（fake 阻塞不响应）：per-call 超时必须切断，
// Deliver 返回 DeadlineExceeded 而非永久阻塞。
func TestDeliverRelayTimeout(t *testing.T) {
	old := relayTimeout
	relayTimeout = 50 * time.Millisecond
	t.Cleanup(func() { relayTimeout = old })

	relayer := &fakeRelayer{fn: func(ctx context.Context, _ int) (*jokerproto.RelayMessageResponse, error) {
		<-ctx.Done() // 模拟对端假死：连接在但不回包，直到 ctx 超时
		return nil, ctx.Err()
	}}
	w := newTestWorker(onlineKv("hang:1"), relayer)

	start := time.Now()
	err := w.Deliver(context.Background(), testPush())
	if err == nil || !strings.Contains(err.Error(), "relay to") {
		t.Fatalf("hanging relay must fail via timeout, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("timeout took %s, per-call bound broken", elapsed)
	}
}

// TestGrpcRelayerEvictsDeadConn 真实 gRPC 连接打向无人监听端口：Relay 报
// Unavailable 后连接必须被驱逐出 conns（不残留、不泄漏后台重连）。
func TestGrpcRelayerEvictsDeadConn(t *testing.T) {
	// 先占后放，确保端口存在但无人监听（connection refused 立即失败）
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	deadAddr := lis.Addr().String()
	lis.Close()

	g := newGrpcRelayer()
	_, err = g.Relay(context.Background(), deadAddr, &jokerproto.RelayMessageRequest{})
	if err == nil {
		t.Fatal("relay to dead addr must fail")
	}
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("dead addr should surface Unavailable, got %v (%s)", err, status.Code(err))
	}
	g.mu.Lock()
	n := len(g.conns)
	g.mu.Unlock()
	if n != 0 {
		t.Fatalf("dead conn must be evicted, conns still has %d entry", n)
	}
}

// TestGrpcRelayerReusesHealthyConn 复用路径回归：同一 addr 成功两次只建一条连接。
func TestGrpcRelayerReusesHealthyConn(t *testing.T) {
	// 起一个最小 relay server：空 manager，收到即回 not_found
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	relay.NewServer(exchange.NewManager()).Register(srv)
	go srv.Serve(lis)
	defer srv.Stop()

	g := newGrpcRelayer()
	req := &jokerproto.RelayMessageRequest{MsgId: "m", ToUid: "nobody"}
	for i := 0; i < 2; i++ {
		resp, err := g.Relay(context.Background(), lis.Addr().String(), req)
		if err != nil {
			t.Fatalf("relay %d: %v", i, err)
		}
		if resp.Reason != "not_found" {
			t.Fatalf("expected not_found from empty manager, got %+v", resp)
		}
	}
	g.mu.Lock()
	n := len(g.conns)
	g.mu.Unlock()
	if n != 1 {
		t.Fatalf("healthy conn must be reused, conns = %d", n)
	}
}

// TestDeliverOfflineNotRetried 离线判定不是错误：Deliver 返回 nil，不进入重试循环的
// 失败路径（否则离线消息会被无意义重投放大）。
func TestDeliverOfflineNotRetried(t *testing.T) {
	relayer := &fakeRelayer{fn: func(context.Context, int) (*jokerproto.RelayMessageResponse, error) {
		t.Fatal("offline push must not reach relay")
		return nil, nil
	}}
	w := newTestWorker(&fakeKv{data: map[string]string{}}, relayer)

	if err := w.deliverWithRetry(context.Background(), testPush()); err != nil {
		t.Fatalf("offline is not an error, got %v", err)
	}
	if got := relayer.count(); got != 0 {
		t.Fatalf("relay called %d times for offline user", got)
	}
}

// TestFlushDeliveredBatchesCallbacks 批处理时 ack 与离线通知各只冲刷一次；
// 两个数组来自分片 goroutine，flush 时统一取走，避免重放导致重复积压。
func TestFlushDeliveredBatchesCallbacks(t *testing.T) {
	w := newTestWorker(&fakeKv{}, nil)
	w.pending = []*chat.Push{testPush(), testPush()}
	w.pendingOffline = []*chat.Push{testPush()}

	var gotAck, gotOffline int
	w.SetBatchDeliveredHook(func(_ context.Context, pushes []*chat.Push) error {
		gotAck += len(pushes)
		return nil
	})
	w.SetBatchOfflineHook(func(_ context.Context, pushes []*chat.Push) error {
		gotOffline += len(pushes)
		return nil
	})
	if err := w.flushDelivered(context.Background()); err != nil {
		t.Fatalf("flush batches: %v", err)
	}
	if gotAck != 2 || gotOffline != 1 {
		t.Fatalf("batched callbacks ack=%d offline=%d, want 2/1", gotAck, gotOffline)
	}
	if len(w.pending) != 0 || len(w.pendingOffline) != 0 {
		t.Fatalf("pending callbacks must be drained, ack=%d offline=%d", len(w.pending), len(w.pendingOffline))
	}
}

// TestFlushDeliveredFallsBackToSingleCallbacks 未接入生产者批钩子时（单测直调
// Deliver 的最小装配）仍逐条调用原回调，保持已有接口语义。
func TestFlushDeliveredFallsBackToSingleCallbacks(t *testing.T) {
	w := newTestWorker(&fakeKv{}, nil)
	w.pending = []*chat.Push{testPush(), testPush()}
	w.pendingOffline = []*chat.Push{testPush()}
	var ack, offline int
	w.onDelivered = func(context.Context, *chat.Push) { ack++ }
	w.onOffline = func(context.Context, *chat.Push) { offline++ }

	if err := w.flushDelivered(context.Background()); err != nil {
		t.Fatalf("flush fallback: %v", err)
	}
	if ack != 2 || offline != 1 {
		t.Fatalf("single callbacks ack=%d offline=%d, want 2/1", ack, offline)
	}
}

// fakeBatchRelayer 记录批调用并按脚本返回结果；单条 Relay 计数验证回退路径。
type fakeBatchRelayer struct {
	mu         sync.Mutex
	batches    []string // 每次批调用的 addr
	batchSizes []int
	singles    int
	failBatch  bool
	results    []*jokerproto.RelayMessageResponse
}

func (f *fakeBatchRelayer) Relay(ctx context.Context, addr string, req *jokerproto.RelayMessageRequest) (*jokerproto.RelayMessageResponse, error) {
	f.mu.Lock()
	f.singles++
	f.mu.Unlock()
	return &jokerproto.RelayMessageResponse{Delivered: true}, nil
}

func (f *fakeBatchRelayer) BatchRelayMessages(ctx context.Context, addr string, req *jokerproto.BatchRelayMessagesRequest) (*jokerproto.BatchRelayMessagesResponse, error) {
	f.mu.Lock()
	f.batches = append(f.batches, addr)
	f.batchSizes = append(f.batchSizes, len(req.Messages))
	f.mu.Unlock()
	if f.failBatch {
		return nil, status.Error(codes.Unavailable, "cs down")
	}
	results := f.results
	if results == nil {
		results = make([]*jokerproto.RelayMessageResponse, len(req.Messages))
		for i := range results {
			results[i] = &jokerproto.RelayMessageResponse{Delivered: true}
		}
	}
	return &jokerproto.BatchRelayMessagesResponse{Results: results}, nil
}

func pushFor(to string) *chat.Push {
	return &chat.Push{MsgID: "m-" + to, CliMsgID: "c-" + to, ConvID: "conv", Seq: 1,
		FromUID: "from", ToUID: to, Type: chat.TypeSingle, Content: map[string]string{"text": "x"}}
}

// TestDeliverBatchGroupsByAddr 同批同目标 CS 只发一次 BatchRelay；送达结果进 pending。
func TestDeliverBatchGroupsByAddr(t *testing.T) {
	kv := &fakeKv{data: map[string]string{
		"online:u1": `{"cs":"cs-1","addr":"csA:37012"}`,
		"online:u2": `{"cs":"cs-2","addr":"csA:37012"}`,
		"online:u3": `{"cs":"cs-3","addr":"csB:37012"}`,
	}}
	relayer := &fakeBatchRelayer{}
	w := newTestWorker(kv, relayer)
	w.collecting = true

	w.deliverBatch(context.Background(), "csA:37012", []*chat.Push{pushFor("u1"), pushFor("u2")})
	w.collecting = false
	if len(relayer.batches) != 1 || relayer.batches[0] != "csA:37012" || relayer.batchSizes[0] != 2 {
		t.Fatalf("batch calls = %v sizes=%v", relayer.batches, relayer.batchSizes)
	}
	if relayer.singles != 0 {
		t.Fatalf("batch success must not fall back to singles, got %d", relayer.singles)
	}
	if len(w.pending) != 2 {
		t.Fatalf("pending = %d, want 2", len(w.pending))
	}
}

// TestDeliverBatchFallbackOnRPCError 批 RPC 失败 → 逐条回退（既有重试路径）。
func TestDeliverBatchFallbackOnRPCError(t *testing.T) {
	kv := onlineKv("csA:37012")
	relayer := &fakeBatchRelayer{failBatch: true}
	w := newTestWorker(kv, relayer)

	w.deliverBatch(context.Background(), "csA:37012", []*chat.Push{pushFor("u1"), pushFor("u1")})
	if len(relayer.batches) != 1 {
		t.Fatalf("batch attempts = %d", len(relayer.batches))
	}
	if relayer.singles != 2 {
		t.Fatalf("fallback singles = %d, want 2", relayer.singles)
	}
}

// TestDeliverBatchNotFoundRetries not_found 结果 → 路由失效后逐条重投。
func TestDeliverBatchNotFoundRetries(t *testing.T) {
	kv := onlineKv("csA:37012")
	relayer := &fakeBatchRelayer{results: []*jokerproto.RelayMessageResponse{
		{Delivered: false, Reason: "not_found"},
	}}
	w := newTestWorker(kv, relayer)

	w.deliverBatch(context.Background(), "csA:37012", []*chat.Push{pushFor("u1")})
	if relayer.singles != 1 {
		t.Fatalf("not_found must trigger single retry, singles = %d", relayer.singles)
	}
}
