// Package relay Joker 的 gRPC 投递接收端（§3 步骤 9-10）：
// deliver worker → RelayMessage → 查本机连接表 → 写入目标连接 Send 通道 → WS 帧。
// Joker 不做任何路由决策（D01 哑管道）：查不到连接就回 not_found，由 worker 兜底。
package relay

import (
	"context"
	"encoding/json"
	"sync/atomic"

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

// AckFrame cmd=ack 下行帧：relay 只负责把 ACK 写入发送者连接。
type AckFrame struct {
	Cmd  string   `json:"cmd"`
	Data chat.Ack `json:"data"`
}

// Server 实现 jokerproto.RelayServer。
type Server struct {
	manager *exchange.Manager
	jokerproto.UnimplementedRelayServer
	messageEnqueued atomic.Int64
	ackEnqueued     atomic.Int64
	sendFull        atomic.Int64
	notFound        atomic.Int64
	marshalError    atomic.Int64
	ctxCanceled     atomic.Int64
	messageBatches  atomic.Int64
	ackBatches      atomic.Int64
}

// Stats 是 relay 运行计数的只读快照，供 pprof/debug 观测与压测采集。
type Stats struct {
	MessageEnqueued int64 `json:"message_enqueued"`
	AckEnqueued     int64 `json:"ack_enqueued"`
	SendFull        int64 `json:"send_full"`
	NotFound        int64 `json:"not_found"`
	MarshalError    int64 `json:"marshal_error"`
	CtxCanceled     int64 `json:"ctx_canceled"`
	MessageBatches  int64 `json:"message_batches"`
	AckBatches      int64 `json:"ack_batches"`
}

func (s *Server) Stats() Stats {
	return Stats{
		MessageEnqueued: s.messageEnqueued.Load(),
		AckEnqueued:     s.ackEnqueued.Load(),
		SendFull:        s.sendFull.Load(),
		NotFound:        s.notFound.Load(),
		MarshalError:    s.marshalError.Load(),
		CtxCanceled:     s.ctxCanceled.Load(),
		MessageBatches:  s.messageBatches.Load(),
		AckBatches:      s.ackBatches.Load(),
	}
}

func NewServer(manager *exchange.Manager) *Server {
	return &Server{manager: manager}
}

// Register 把 gRPC 服务挂到 server（由 joker.Start 调用）。
func (s *Server) Register(gs *grpc.Server) {
	jokerproto.RegisterRelayServer(gs, s)
}

// RelayMessage 本地投递：查连接表 → Send 通道 → WS 帧。
// Send 满立即返回失败，不等待 3 秒；慢客户端由 seq 补拉兜底，避免拖住整批 relay。
func (s *Server) RelayMessage(ctx context.Context, req *jokerproto.RelayMessageRequest) (*jokerproto.RelayMessageResponse, error) {
	if req.IsLocal {
		log.Warnf("relay: is_local=true unexpected from %s", req.MsgId)
	}
	return s.enqueueMessage(ctx, req)
}

func (s *Server) enqueueMessage(ctx context.Context, req *jokerproto.RelayMessageRequest) (*jokerproto.RelayMessageResponse, error) {
	client := s.manager.GetClient(req.ToUid)
	if client == nil {
		s.notFound.Add(1)
		return &jokerproto.RelayMessageResponse{Delivered: false, Reason: "not_found"}, nil
	}
	frame := DeliveryFrame{
		Cmd: "msg",
		Data: chat.Push{
			MsgID: req.MsgId, CliMsgID: req.CliMsgId, ConvID: req.ConvId, Seq: req.Seq,
			FromUID: req.FromUid, ToUID: req.ToUid, Type: req.Type,
			Content: json.RawMessage(req.Content), Mention: req.Mention,
		},
	}
	raw, err := json.Marshal(frame)
	if err != nil {
		s.marshalError.Add(1)
		log.Errorf("relay: marshal frame %s: %v", req.MsgId, err)
		return &jokerproto.RelayMessageResponse{Delivered: false, Reason: "marshal_error"}, nil
	}
	select {
	case client.Send <- raw:
		s.messageEnqueued.Add(1)
		return &jokerproto.RelayMessageResponse{Delivered: true}, nil
	case <-ctx.Done():
		s.ctxCanceled.Add(1)
		return &jokerproto.RelayMessageResponse{Delivered: false, Reason: "ctx_canceled"}, nil
	default:
		s.sendFull.Add(1)
		return &jokerproto.RelayMessageResponse{Delivered: false, Reason: "send_full"}, nil
	}
}

// BatchRelayMessages 按请求顺序写入目标连接；results 与 messages 一一对应。
func (s *Server) BatchRelayMessages(ctx context.Context, req *jokerproto.BatchRelayMessagesRequest) (*jokerproto.BatchRelayMessagesResponse, error) {
	s.messageBatches.Add(1)
	out := &jokerproto.BatchRelayMessagesResponse{Results: make([]*jokerproto.RelayMessageResponse, 0, len(req.Messages))}
	for _, item := range req.Messages {
		result, err := s.enqueueMessage(ctx, item)
		if err != nil {
			return nil, err
		}
		out.Results = append(out.Results, result)
	}
	return out, nil
}

// BatchRelayAcks 输出 cmd=ack 帧，不能复用消息 relay 的 cmd=msg 载荷。
func (s *Server) BatchRelayAcks(ctx context.Context, req *jokerproto.BatchRelayAcksRequest) (*jokerproto.BatchRelayAcksResponse, error) {
	s.ackBatches.Add(1)
	out := &jokerproto.BatchRelayAcksResponse{Results: make([]*jokerproto.RelayMessageResponse, 0, len(req.Acks))}
	for _, ack := range req.Acks {
		result, err := s.enqueueAck(ctx, ack)
		if err != nil {
			return nil, err
		}
		out.Results = append(out.Results, result)
	}
	return out, nil
}

func (s *Server) enqueueAck(ctx context.Context, req *jokerproto.RelayAckRequest) (*jokerproto.RelayMessageResponse, error) {
	client := s.manager.GetClient(req.FromUid)
	if client == nil {
		s.notFound.Add(1)
		return &jokerproto.RelayMessageResponse{Delivered: false, Reason: "not_found"}, nil
	}
	raw, err := json.Marshal(AckFrame{Cmd: "ack", Data: chat.Ack{
		MsgID: req.MsgId, CliMsgID: req.CliMsgId, ConvID: req.ConvId,
		Seq: req.Seq, FromUID: req.FromUid, ToUID: req.ToUid,
	}})
	if err != nil {
		s.marshalError.Add(1)
		return &jokerproto.RelayMessageResponse{Delivered: false, Reason: "marshal_error"}, nil
	}
	select {
	case client.Send <- raw:
		s.ackEnqueued.Add(1)
		return &jokerproto.RelayMessageResponse{Delivered: true}, nil
	case <-ctx.Done():
		s.ctxCanceled.Add(1)
		return &jokerproto.RelayMessageResponse{Delivered: false, Reason: "ctx_canceled"}, nil
	default:
		s.sendFull.Add(1)
		return &jokerproto.RelayMessageResponse{Delivered: false, Reason: "send_full"}, nil
	}
}
