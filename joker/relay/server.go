// Package relay Joker 的 gRPC 投递接收端（§3 步骤 9-10）：
// deliver worker → RelayMessage → 查本机连接表 → 写入目标连接 Send 通道 → WS 帧。
// Joker 不做任何路由决策（D01 哑管道）：查不到连接就回 not_found，由 worker 兜底。
package relay

import (
	"context"
	"encoding/json"
	"time"

	"alfred.brave.com/event"
	"alfred.brave.com/internal/chat"
	"alfred.brave.com/joker/exchange"
	"alfred.brave.com/joker/proto"

	"google.golang.org/grpc"
)

var log = event.Log

// DeliveryFrame cmd=msg 下行帧：与 chat.Push 载荷一致，客户端按 seq 排序去重（D15）。
type DeliveryFrame struct {
	Cmd  string    `json:"cmd"`
	Data chat.Push `json:"data"`
}

// Server 实现 jokerproto.RelayServer。
type Server struct {
	manager *exchange.Manager
	jokerproto.UnimplementedRelayServer
}

func NewServer(manager *exchange.Manager) *Server {
	return &Server{manager: manager}
}

// Register 把 gRPC 服务挂到 server（由 joker.Start 调用）。
func (s *Server) Register(gs *grpc.Server) {
	jokerproto.RegisterRelayServer(gs, s)
}

// RelayMessage 本地投递：查连接表 → Send 通道 → WS 帧。
// Send 通道满（写超时 3s）按投递失败处理——客户端慢消费不该拖死 worker（D20 环节④变体）。
func (s *Server) RelayMessage(ctx context.Context, req *jokerproto.RelayMessageRequest) (*jokerproto.RelayMessageResponse, error) {
	if req.IsLocal {
		// 契约审查位：当前架构投递恒单向（worker→CS），is_local=true 视为异常调用
		log.Warnf("relay: is_local=true unexpected from %s", req.MsgId)
	}
	client := s.manager.GetClient(req.ToUid)
	if client == nil {
		return &jokerproto.RelayMessageResponse{Delivered: false, Reason: "not_found"}, nil
	}

	frame := DeliveryFrame{
		Cmd: "msg",
		Data: chat.Push{
			MsgID:    req.MsgId,
			CliMsgID: req.CliMsgId,
			ConvID:   req.ConvId,
			Seq:      req.Seq,
			FromUID:  req.FromUid,
			ToUID:    req.ToUid,
			Type:     req.Type,
			Content:  json.RawMessage(req.Content),
			Mention:  req.Mention,
		},
	}
	raw, err := json.Marshal(frame)
	if err != nil {
		log.Errorf("relay: marshal frame %s: %v", req.MsgId, err)
		return &jokerproto.RelayMessageResponse{Delivered: false, Reason: "marshal_error"}, nil
	}

	select {
	case client.Send <- raw:
		return &jokerproto.RelayMessageResponse{Delivered: true}, nil
	case <-time.After(3 * time.Second):
		log.Warnf("relay: send chan full, drop %s to %s", req.MsgId, req.ToUid)
		return &jokerproto.RelayMessageResponse{Delivered: false, Reason: "send_full"}, nil
	case <-ctx.Done():
		return &jokerproto.RelayMessageResponse{Delivered: false, Reason: "ctx_canceled"}, nil
	}
}
