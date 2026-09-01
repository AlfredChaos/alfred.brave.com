//go:build integration

// 集成测试：进程内起 relay gRPC server + 真实 PG kv → deliver.Deliver 闭环。
// 前置：PG @127.0.0.1:55432。
package deliver

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"alfred.brave.com/database"
	"alfred.brave.com/internal/chat"
	"alfred.brave.com/joker/exchange"
	"alfred.brave.com/joker/relay"

	"google.golang.org/grpc"
)

type env struct {
	store   *database.Store
	kv      *database.KvStore
	worker  *Worker
	servers []*csNode // 多个进程内 CS 节点，模拟路由漂移
}

// csNode 一个进程内 Chat Server：独立 Manager + gRPC relay。
type csNode struct {
	manager *exchange.Manager
	server  *grpc.Server
	addr    string
}

func startEnv(t *testing.T, nodes int) *env {
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
	kv := database.NewKvStore(store)

	e := &env{store: store, kv: kv, servers: make([]*csNode, 0, nodes)}
	for i := 0; i < nodes; i++ {
		manager := exchange.NewManager()
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		server := grpc.NewServer()
		relay.NewServer(manager).Register(server)
		go server.Serve(lis)
		t.Cleanup(server.Stop)
		e.servers = append(e.servers, &csNode{manager: manager, server: server, addr: lis.Addr().String()})
	}

	// brokers 仅用于 reader 构造；本测试直接调 Deliver，不跑消费循环
	w := New(store, []string{"127.0.0.1:9092"}, "deliver-it", nil, nil)
	w.relayer = newGrpcRelayer()
	e.worker = w
	return e
}

func readFrame(t *testing.T, c *exchange.Client) chat.Push {
	t.Helper()
	select {
	case raw := <-c.Send:
		var frame struct {
			Cmd  string    `json:"cmd"`
			Data chat.Push `json:"data"`
		}
		if err := json.Unmarshal(raw, &frame); err != nil {
			t.Fatalf("unmarshal frame %s: %v", raw, err)
		}
		if frame.Cmd != "msg" {
			t.Fatalf("cmd = %s, want msg", frame.Cmd)
		}
		return frame.Data
	case <-time.After(3 * time.Second):
		t.Fatal("no frame delivered")
		return chat.Push{}
	}
}

// TestDeliverClosure 在线投递闭环：kv 登记 online → Deliver → 目标连接收到 WS 帧。
func TestDeliverClosure(t *testing.T) {
	e := startEnv(t, 1)
	ctx := context.Background()
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())

	uid := "dlv-a-" + suffix
	client := exchange.NewClient(e.servers[0].manager, uid, nil)
	e.servers[0].manager.EventRegister(client) // 测试直接写 kv（绕过 online 依赖）
	if err := e.kv.Put(ctx, "online:"+uid, OnlineAddr{Cs: "cs-0", Addr: e.servers[0].addr}); err != nil {
		t.Fatalf("kv put: %v", err)
	}

	push := &chat.Push{
		MsgID: "m-1", CliMsgID: "c-1", ConvID: "conv-1", Seq: 7,
		FromUID: "sender", ToUID: uid, Type: chat.TypeSingle,
		Content: map[string]string{"text": "hello"},
	}
	if err := e.worker.Deliver(ctx, push); err != nil {
		t.Fatalf("deliver: %v", err)
	}
	got := readFrame(t, client)
	if got.MsgID != "m-1" || got.Seq != 7 || got.FromUID != "sender" {
		t.Fatalf("frame mismatch: %+v", got)
	}
}

// TestDeliverOfflineSkip 离线：无 online 记录 → 不报错不投递（补拉兜底）。
func TestDeliverOfflineSkip(t *testing.T) {
	e := startEnv(t, 1)
	ctx := context.Background()

	push := &chat.Push{MsgID: "m-2", ConvID: "conv", Seq: 1, FromUID: "a", ToUID: "nobody-here", Type: chat.TypeSingle, Content: map[string]string{"text": "x"}}
	if err := e.worker.Deliver(ctx, push); err != nil {
		t.Fatalf("offline must be a no-op, got %v", err)
	}
}

// TestDeliverStaleCacheCorrection 防线②：缓存指向旧 CS（uid 连接已迁走）→ 首投 not_found
// → 强刷缓存 → 权威值指向新 CS → 重投成功。两个独立 gRPC 节点，真漂移场景。
func TestDeliverStaleCacheCorrection(t *testing.T) {
	e := startEnv(t, 2)
	ctx := context.Background()
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	uid := "dlv-b-" + suffix

	// 用户曾连在 cs[0]，后漂移到 cs[1]（kv 权威已指向 cs[1]，deliver 缓存停留在 cs[0]）
	old := exchange.NewClient(e.servers[0].manager, uid, nil)
	e.servers[0].manager.EventRegister(old)
	if err := e.kv.Put(ctx, "online:"+uid, OnlineAddr{Cs: "cs-0", Addr: e.servers[0].addr}); err != nil {
		t.Fatalf("kv put: %v", err)
	}
	if _, err := e.worker.router.Get(ctx, uid); err != nil { // 预热缓存 → cs[0]
		t.Fatalf("warm cache: %v", err)
	}
	e.servers[0].manager.EventUnregister(old) // 旧连接断开（cs[0] 上不再有该 uid）
	fresh := exchange.NewClient(e.servers[1].manager, uid, nil)
	e.servers[1].manager.EventRegister(fresh) // 漂移到 cs[1]
	if err := e.kv.Put(ctx, "online:"+uid, OnlineAddr{Cs: "cs-1", Addr: e.servers[1].addr}); err != nil {
		t.Fatalf("kv put drift: %v", err)
	}

	push := &chat.Push{MsgID: "m-3", ConvID: "conv", Seq: 3, FromUID: "a", ToUID: uid, Type: chat.TypeSingle, Content: map[string]string{"text": "y"}}
	if err := e.worker.Deliver(ctx, push); err != nil {
		t.Fatalf("deliver with stale route: %v", err)
	}
	got := readFrame(t, fresh)
	if got.MsgID != "m-3" {
		t.Fatalf("frame mismatch: %+v", got)
	}
}
