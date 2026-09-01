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

	"alfred.brave.com/database"
	"alfred.brave.com/event"
	"alfred.brave.com/internal/chat"
	"alfred.brave.com/joker/proto"

	"github.com/segmentio/kafka-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var log = event.Log

// Relayer gRPC 客户端抽象（测试注入 fake）。
type Relayer interface {
	Relay(ctx context.Context, addr string, req *jokerproto.RelayMessageRequest) (*jokerproto.RelayMessageResponse, error)
}

// grpcRelayer 真实实现：按 addr 维护连接复用（懒建 + 进程生命周期持有）。
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
	c, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("grpc dial %s: %w", addr, err)
	}
	g.conns[addr] = c
	return c, nil
}

func (g *grpcRelayer) Relay(ctx context.Context, addr string, req *jokerproto.RelayMessageRequest) (*jokerproto.RelayMessageResponse, error) {
	conn, err := g.conn(addr)
	if err != nil {
		return nil, err
	}
	return jokerproto.NewRelayClient(conn).RelayMessage(ctx, req)
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
		if err := w.Deliver(ctx, &push); err != nil {
			// 投递基础设施错误：不 commit，重投（幂等靠 msg_id）
			log.Errorf("deliver: handle %s failed (will retry): %v", push.MsgID, err)
			if ctx.Err() == nil {
				continue // 本轮跳过 commit，消息将在下次 fetch 重投（进程不退出，避免毒消息放大）
			}
			return err
		}
		if err := w.reader.CommitMessages(ctx, m); err != nil {
			return err
		}
	}
}

// Deliver 投递一条 chat.push（可直接被测试调用）。
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
	resp, err := w.relayer.Relay(ctx, addr.Addr, req)
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
		resp, err = w.relayer.Relay(ctx, addr.Addr, req)
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
