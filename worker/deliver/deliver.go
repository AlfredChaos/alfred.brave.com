// Package deliver 投递 worker（§3 步骤 7-12）：消费 chat.push → 查 online kv（30s TTL 缓存）
// → gRPC RelayMessage → 目标 CS → Send 通道 → WS 帧。
//
// 三道失效防线（D06）：① 缓存 TTL 30s 上界；② not-found 即时校正（强刷缓存重查重投一次）；
// ③ LISTEN/NOTIFY 加速为可选项不实现。离线判定是投递查路由的免费副产品（D12）。
package deliver

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"alfred.brave.com/database"
	"alfred.brave.com/event"
	"alfred.brave.com/internal/chat"
	"alfred.brave.com/joker/proto"

	"github.com/segmentio/kafka-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

var log = event.Log

// relayTimeout 单次 RelayMessage 调用的时间上界。下界由 server 端定（Send 通道满 3s
// 弃投，D20 环节④），client 再留 2s 余量覆盖建连与往返。它是抗 CS 挂起的第一道防线：
// Run 是单 goroutine 串行消费，一个无 deadline 的 RPC 卡在对端假死（TCP 半开）上，
// 拖死的是整个 worker 的全部分区投递。var 形式便于测试注入缩短。
var relayTimeout = 5 * time.Second

// 重试策略：投递失败多为瞬时（CS 重启窗口、路由缓存 30s 滞后于权威值、网络抖动），
// 少量同步退避即可穿越；仍失败则显式跳过（见 Run），靠客户端补拉兜底（D15）。
const (
	relayAttempts = 3
	relayBackoff  = 500 * time.Millisecond
)

// Relayer gRPC 客户端抽象（测试注入 fake）。
type Relayer interface {
	Relay(ctx context.Context, addr string, req *jokerproto.RelayMessageRequest) (*jokerproto.RelayMessageResponse, error)
}

// BatchRelayer 批量投递能力；实现同时保留 Relayer 以便单条重试和测试 fake。
type BatchRelayer interface {
	Relayer
	BatchRelayMessages(ctx context.Context, addr string, req *jokerproto.BatchRelayMessagesRequest) (*jokerproto.BatchRelayMessagesResponse, error)
}

// grpcRelayer 真实实现：按 addr 维护连接复用（懒建 + 连接级失败驱逐）。
// 并发说明：Run 是单 goroutine 串行消费，evict 不存在"正在使用的连接被并发关闭"竞争；
// 若未来消费并发化，需改为引用计数后才能 Close。
type grpcRelayer struct {
	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
}

func newGrpcRelayer() *grpcRelayer {
	return &grpcRelayer{conns: make(map[string]*grpc.ClientConn)}
}

func (g *grpcRelayer) conn(addr string) (*grpc.ClientConn, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if c, ok := g.conns[addr]; ok {
		return c, nil
	}
	// keepalive 第二道防线（配合 relayTimeout）：对端假死/网络黑洞时 TCP 不关、
	// gRPC 默认不发探测 ping，坏连接永远检测不出来。PermitWithoutStream 必须
	// 打开——relay 是离散 unary 调用，绝大多数时间连接上没有活跃流。
	// server 端需同步放宽 EnforcementPolicy（joker/start.go），否则 20s 的无流
	// ping 会被默认 5min MinTime 判为恶意而踢连接。
	c, err := grpc.Dial(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                20 * time.Second,
			Timeout:             5 * time.Second,
			PermitWithoutStream: true,
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("grpc dial %s: %w", addr, err)
	}
	g.conns[addr] = c
	return c, nil
}

// evict 驱逐并关闭 addr 的连接：目标 CS 已下线时，坏连接留在 conns 里除了让
// 后续请求反复撞它，还造成节点地址漂移/永久下线后的连接泄漏（gRPC 后台无限重连）。
func (g *grpcRelayer) evict(addr string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if c, ok := g.conns[addr]; ok {
		delete(g.conns, addr)
		if err := c.Close(); err != nil {
			log.Warnf("close evicted grpc conn %s: %v", addr, err)
		}
	}
}

func (g *grpcRelayer) Relay(ctx context.Context, addr string, req *jokerproto.RelayMessageRequest) (*jokerproto.RelayMessageResponse, error) {
	conn, err := g.conn(addr)
	if err != nil {
		return nil, err
	}
	resp, err := jokerproto.NewRelayClient(conn).RelayMessage(ctx, req)
	if err != nil && status.Code(err) == codes.Unavailable {
		// 连接级失败（CS 宕机/不可达）：驱逐后下次重新建连（重新解析 DNS），
		// 同 addr 短暂重启场景由 gRPC 内建重连兜底，驱逐只做回收与语义收口
		log.Warnf("deliver: evict grpc conn %s (unavailable: %v)", addr, err)
		g.evict(addr)
	}
	return resp, err
}

func (g *grpcRelayer) BatchRelayMessages(ctx context.Context, addr string, req *jokerproto.BatchRelayMessagesRequest) (*jokerproto.BatchRelayMessagesResponse, error) {
	conn, err := g.conn(addr)
	if err != nil {
		return nil, err
	}
	resp, err := jokerproto.NewRelayClient(conn).BatchRelayMessages(ctx, req)
	if err != nil && status.Code(err) == codes.Unavailable {
		log.Warnf("deliver: evict grpc conn %s (batch unavailable: %v)", addr, err)
		g.evict(addr)
	}
	return resp, err
}

func (g *grpcRelayer) Close() {
	g.mu.Lock()
	defer g.mu.Unlock()
	for addr, c := range g.conns {
		if err := c.Close(); err != nil {
			log.Warnf("close grpc conn %s: %v", addr, err)
		}
	}
}

// Worker deliver 消费者。
type Worker struct {
	router  *Router
	kv      *database.KvStore
	relayer Relayer
	reader  *kafka.Reader
	// onDelivered 送达后回调（T07 挂 chat.ack 生产；测试注入）
	onDelivered func(ctx context.Context, push *chat.Push)
	// onOffline 离线回调（T16 挂 chat.notify 生产；当前仅日志）
	onOffline func(ctx context.Context, push *chat.Push)
	// S3c 批量化：collecting=true 期间送达的 push 入 pending，批尾由 batchDelivered
	// 一次写出（chat.ack 的逐条 acks=all 跨机确认税，与 persist flushPushes 同思路）。
	// pending 由 16 个 shard goroutine 并发 append，需要用锁；冲刷在 wg.Wait 之后
	// 且 collecting 已复位，单协程访问无需锁。
	collecting     bool
	pendingMu      sync.Mutex
	pending        []*chat.Push
	batchDelivered func(ctx context.Context, pushes []*chat.Push) error
	// offline 通知同样批量化：全部积压离线时（深夜通知风暴/风暴后清积压）逐条
	// chat.notify produce 会把 deliver 钉回 ~200/s（S3c 实测），批尾一次写摊掉。
	pendingOffline []*chat.Push
	batchOffline   func(ctx context.Context, pushes []*chat.Push) error
}

// SetBatchDeliveredHook 注入送达批回调。未设置（如集成测试直调 Deliver）时
// Run 批尾逐条回退调用 onDelivered，语义与旧版一致。
func (w *Worker) SetBatchDeliveredHook(fn func(ctx context.Context, pushes []*chat.Push) error) {
	w.batchDelivered = fn
}

// SetBatchOfflineHook 注入离线通知批回调；未设置时批尾逐条回退 onOffline。
func (w *Worker) SetBatchOfflineHook(fn func(ctx context.Context, pushes []*chat.Push) error) {
	w.batchOffline = fn
}

// New 构造 worker。onDelivered/onOffline 可为 nil。
func New(store *database.Store, brokers []string, groupID string, onDelivered, onOffline func(context.Context, *chat.Push)) *Worker {
	kv := database.NewKvStore(store)
	w := &Worker{
		router:  NewRouter(kv),
		kv:      kv,
		relayer: newGrpcRelayer(),
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers: brokers,
			GroupID: groupID,
			Topic:   chat.TopicPush,
		}),
		onDelivered: onDelivered,
		onOffline:   onOffline,
	}
	if onDelivered == nil {
		w.onDelivered = func(context.Context, *chat.Push) {}
	}
	if onOffline == nil {
		w.onOffline = func(context.Context, *chat.Push) {}
	}
	return w
}

// Run 消费循环（S3c 批量化）：批拉 → 按 ToUID 分片并发 relay → 一次批 commit
// （at-least-once；CS 端/客户端按 msg_id 幂等）。
// 单条串行路径（fetch→gRPC→commit 逐条往返）实测把投递链钉在 ~117/s（S3）；
// 16 分片并发 + 批提交摊薄，分片内 FIFO 保用户级投递顺序。
func (w *Worker) Run(ctx context.Context) error {
	log.Infof("deliver: consuming %s (group=%s) [sharded-batch]", chat.TopicPush, w.reader.Config().GroupID)
	const (
		batchMax  = 512
		batchWait = 200 * time.Millisecond
	)
	for {
		m, err := w.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				log.Info("deliver: consumer stopped")
				return nil
			}
			return fmt.Errorf("deliver: fetch: %w", err)
		}
		batch := []kafka.Message{m}
		dctx, dcancel := context.WithTimeout(ctx, batchWait)
		for len(batch) < batchMax {
			m2, derr := w.reader.FetchMessage(dctx)
			if derr != nil {
				break // 攒批窗口到：低流量时小批也出批
			}
			batch = append(batch, m2)
		}
		dcancel()

		// 按目标 CS 地址聚批：同一 batch 对同一 CS 只发一次 unary BatchRelay。
		// chat.push key=to_uid，Kafka 仍保证同一接收者同分区 FIFO；每个地址列表
		// 保持 reader 返回的顺序，避免在本批内重排同一接收者的消息。
		parsed := make([]*chat.Push, 0, len(batch))
		for _, bm := range batch {
			var push chat.Push
			if err := json.Unmarshal(bm.Value, &push); err != nil {
				log.Errorf("deliver: bad payload (partition=%d offset=%d): %v", bm.Partition, bm.Offset, err)
				continue
			}
			parsed = append(parsed, &push)
		}
		w.collecting = true
		groups := make(map[string][]*chat.Push)
		for _, p := range parsed {
			addr, err := w.router.Get(ctx, p.ToUID)
			if err != nil {
				log.Errorf("deliver: route %s failed: %v", p.ToUID, err)
				continue
			}
			if addr.Addr == "" {
				_ = w.offlineOrQueue(ctx, p)
				continue
			}
			groups[addr.Addr] = append(groups[addr.Addr], p)
		}
		var wg sync.WaitGroup
		for addr, list := range groups {
			wg.Add(1)
			go func(addr string, list []*chat.Push) {
				defer wg.Done()
				w.deliverBatch(ctx, addr, list)
			}(addr, list)
		}
		wg.Wait()
		w.collecting = false
		if err := w.flushDelivered(ctx); err != nil {
			return err
		}
		if err := w.reader.CommitMessages(ctx, batch...); err != nil {
			return fmt.Errorf("deliver: commit: %w", err)
		}
	}
}

// deliverBatch 对同一目标 CS 发一次 BatchRelay；RPC 级失败或协议不完整时逐条
// 回退到既有 retry 路径，局部失败则仅处理对应消息，不让一条 send_full 拖住整批。
func (w *Worker) deliverBatch(ctx context.Context, addr string, list []*chat.Push) {
	batchRelayer, ok := w.relayer.(BatchRelayer)
	if !ok {
		w.deliverIndividually(ctx, list)
		return
	}
	reqs := make([]*jokerproto.RelayMessageRequest, 0, len(list))
	for _, p := range list {
		reqs = append(reqs, relayRequest(p, 0))
	}
	callCtx, cancel := context.WithTimeout(ctx, relayTimeout)
	response, err := batchRelayer.BatchRelayMessages(callCtx, addr, &jokerproto.BatchRelayMessagesRequest{Messages: reqs})
	cancel()
	if err != nil || response == nil || len(response.Results) != len(list) {
		if err != nil {
			log.Warnf("deliver: batch relay to %s failed (%d messages): %v; fallback per message", addr, len(list), err)
		} else {
			log.Warnf("deliver: batch relay to %s returned incomplete results; fallback per message", addr)
		}
		w.deliverIndividually(ctx, list)
		return
	}
	for i, result := range response.Results {
		push := list[i]
		if result.Delivered {
			w.pendingMu.Lock()
			w.pending = append(w.pending, push)
			w.pendingMu.Unlock()
			continue
		}
		if result.Reason == "not_found" {
			w.router.Invalidate(push.ToUID)
			w.deliverIndividually(ctx, []*chat.Push{push})
			continue
		}
		log.Warnf("deliver: %s not delivered to %s (%s), rely on pull-back", push.MsgID, push.ToUID, result.Reason)
	}
}

func (w *Worker) deliverIndividually(ctx context.Context, list []*chat.Push) {
	for _, push := range list {
		if err := w.deliverWithRetry(ctx, push); err != nil {
			log.Errorf("deliver: drop %s to %s after %d attempts: %v (pull-back fallback)",
				push.MsgID, push.ToUID, relayAttempts, err)
		}
	}
}

// flushDelivered 批尾冲刷送达回调：优先 batchDelivered 一次写出；未注入批钩子时
// 逐条回退 onDelivered。失败返回错误 → 整批不提交重投（at-least-once，
// CS/客户端按 msg_id 幂等，重复 ack 无害）。
func (w *Worker) flushDelivered(ctx context.Context) error {
	w.pendingMu.Lock()
	pending := w.pending
	w.pending = nil
	pendingOff := w.pendingOffline
	w.pendingOffline = nil
	w.pendingMu.Unlock()
	if w.batchDelivered != nil {
		if len(pending) > 0 {
			if err := w.batchDelivered(ctx, pending); err != nil {
				return fmt.Errorf("deliver: flush acks batch(%d): %w", len(pending), err)
			}
		}
	} else {
		for _, p := range pending {
			w.onDelivered(ctx, p)
		}
	}
	if w.batchOffline != nil {
		if len(pendingOff) > 0 {
			if err := w.batchOffline(ctx, pendingOff); err != nil {
				return fmt.Errorf("deliver: flush notifies batch(%d): %w", len(pendingOff), err)
			}
		}
	} else {
		for _, p := range pendingOff {
			w.offline(ctx, p)
		}
	}
	return nil
}

// fnv32 轻量哈希（分片路由用，无碰撞正确性要求——碰撞仅影响并发度不影响正确性）。
func fnv32(s string) uint32 {
	const (
		offset32 = 2166136261
		prime32  = 16777619
	)
	h := uint32(offset32)
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= prime32
	}
	return h
}

// deliverWithRetry 进程内同步退避重试。重试前先 Invalidate 路由缓存：CS 宕机后
// 用户重连漂移到新节点时，30s TTL 缓存里的旧地址正是首投失败的根源，强刷后重试
// 直读权威 kv 才能拿到新地址。
func (w *Worker) deliverWithRetry(ctx context.Context, push *chat.Push) error {
	var err error
	for attempt := 1; attempt <= relayAttempts; attempt++ {
		if attempt > 1 {
			w.router.Invalidate(push.ToUID)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(relayBackoff << uint(attempt-1)):
			}
		}
		if err = w.Deliver(ctx, push); err == nil {
			return nil
		}
		log.Warnf("deliver: attempt %d/%d for %s failed: %v", attempt, relayAttempts, push.MsgID, err)
	}
	return err
}

// Deliver 投递一条 chat.push（可直接被测试调用）。每次 relay 调用单独限时。
func (w *Worker) Deliver(ctx context.Context, push *chat.Push) error {
	req := relayRequest(push, 0)

	// 首投：走 30s TTL 缓存
	addr, err := w.router.Get(ctx, push.ToUID)
	if err != nil {
		return err
	}
	if addr.Addr == "" {
		return w.offlineOrQueue(ctx, push)
	}
	resp, err := w.relayWithTimeout(ctx, addr.Addr, req)
	if err != nil {
		return fmt.Errorf("relay to %s: %w", addr.Addr, err)
	}

	// 防线②：not-found 即时校正——强刷缓存直查权威值，重投一次
	if !resp.Delivered && resp.Reason == "not_found" {
		w.router.Invalidate(push.ToUID)
		addr, err = w.router.Get(ctx, push.ToUID)
		if err != nil {
			return err
		}
		if addr.Addr == "" {
			return w.offlineOrQueue(ctx, push)
		}
		resp, err = w.relayWithTimeout(ctx, addr.Addr, req)
		if err != nil {
			return fmt.Errorf("relay retry to %s: %w", addr.Addr, err)
		}
	}

	if resp.Delivered {
		if w.collecting {
			w.pendingMu.Lock()
			w.pending = append(w.pending, push)
			w.pendingMu.Unlock()
			return nil
		}
		w.onDelivered(ctx, push)
		return nil
	}
	// send_full 等非路由原因：按投递失败计，不重投（客户端靠补拉兜底）
	log.Warnf("deliver: %s not delivered to %s (%s), rely on pull-back", push.MsgID, push.ToUID, resp.Reason)
	return nil
}

// relayWithTimeout 单次 relay 上限 relayTimeout：防对端假死（TCP 半开）时无 deadline
// 的 RPC 永久挂起，拖死串行消费循环（keepalive 是加速检测的第二道防线）。
func (w *Worker) relayWithTimeout(ctx context.Context, addr string, req *jokerproto.RelayMessageRequest) (*jokerproto.RelayMessageResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, relayTimeout)
	defer cancel()
	return w.relayer.Relay(ctx, addr, req)
}

// offline 离线分支（D12）：免费副产品，触发通知回调（T16 起为 chat.notify）。
func (w *Worker) offline(ctx context.Context, push *chat.Push) error {
	log.Debugf("deliver: %s offline, skip push (conv=%s seq=%d)", push.ToUID, push.ConvID, push.Seq)
	w.onOffline(ctx, push)
	return nil
}

// offlineOrQueue 批处理期间离线通知入队（同 onDelivered 批量化，见 flushDelivered）；
// 非批路径（测试直调 Deliver）维持逐条语义。
func (w *Worker) offlineOrQueue(ctx context.Context, push *chat.Push) error {
	if w.collecting {
		w.pendingMu.Lock()
		w.pendingOffline = append(w.pendingOffline, push)
		w.pendingMu.Unlock()
		return nil
	}
	return w.offline(ctx, push)
}

func relayRequest(push *chat.Push, createdAt int64) *jokerproto.RelayMessageRequest {
	content, _ := json.Marshal(push.Content)
	return &jokerproto.RelayMessageRequest{
		MsgId:    push.MsgID,
		CliMsgId: push.CliMsgID,
		ConvId:   push.ConvID,
		FromUid:  push.FromUID,
		ToUid:    push.ToUID,
		Type:     push.Type,
		Content:  content,
		Seq:      push.Seq,
		Mention:  push.Mention,
	}
}
