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

// Run 消费循环：fetch → Deliver → commit（at-least-once；CS 端/客户端按 msg_id 幂等）。
func (w *Worker) Run(ctx context.Context) error {
	log.Infof("deliver: consuming %s (group=%s)", chat.TopicPush, w.reader.Config().GroupID)
	for {
		m, err := w.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				log.Info("deliver: consumer stopped")
				return nil
			}
			return fmt.Errorf("deliver: fetch: %w", err)
		}
		var push chat.Push
		if err := json.Unmarshal(m.Value, &push); err != nil {
			log.Errorf("deliver: bad payload (partition=%d offset=%d): %v", m.Partition, m.Offset, err)
			if err := w.reader.CommitMessages(ctx, m); err != nil {
				return err
			}
			continue
		}
		if err := w.deliverWithRetry(ctx, &push); err != nil {
			// 有限重试仍失败：显式 commit 跳过本条。此前"不 commit 等重投"是
			// 假语义——kafka-go 按最高 offset 提交（后一条成功的 commit 会把本条
			// 一并带过），消息根本留不住；不如如实记录：未送达的推送由客户端
			// 按 seq 补拉兜底（D15），同时避免 committed offset 落后在进程重启时
			// 放大成大批量重复消费。
			log.Errorf("deliver: drop %s to %s after %d attempts: %v (pull-back fallback)",
				push.MsgID, push.ToUID, relayAttempts, err)
		}
		if err := w.reader.CommitMessages(ctx, m); err != nil {
			return err
		}
	}
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
		return w.offline(ctx, push)
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
			return w.offline(ctx, push)
		}
		resp, err = w.relayWithTimeout(ctx, addr.Addr, req)
		if err != nil {
			return fmt.Errorf("relay retry to %s: %w", addr.Addr, err)
		}
	}

	if resp.Delivered {
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
