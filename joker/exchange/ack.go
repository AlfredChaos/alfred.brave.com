package exchange

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"alfred.brave.com/event"
	"alfred.brave.com/internal/chat"
	"alfred.brave.com/joker/proto"
	"alfred.brave.com/joker/relayclient"

	"github.com/segmentio/kafka-go"
)

var ackLog = event.Log

const (
	ackBatchMax  = 512
	ackBatchWait = 200 * time.Millisecond
)

// AckFrame cmd=ack 下行帧：送达回执推给发送者（§3 步骤 13-15）。
// 客户端按 cli_msg_id 把消息状态从“已受理”翻为“已送达”（打勾），按 msg_id 幂等。
type AckFrame struct {
	Cmd  string   `json:"cmd"`
	Data chat.Ack `json:"data"`
}

// AckRelayer ACK 跨 CS 转发抽象（测试注入 fake）。
type AckRelayer interface {
	BatchRelayAcks(ctx context.Context, addr string, req *jokerproto.BatchRelayAcksRequest) (*jokerproto.BatchRelayAcksResponse, error)
}

// ConsumeAcks 消费 chat.ack 并推给发送者所在 CS。
//
// 消费组语义：所有 CS 共享稳定组 "cs-ack"。每条 ACK 只被一个实例处理一次；
// 本机发送者直接入 Send 通道，异机按 online:{from_uid}.addr 批转发。
// 旧广播组 cs-ack-{ServiceId} 会让每台 CS 都串行吃全量 ACK，S3c 实测 ack p50≈14s。
func ConsumeAcks(ctx context.Context, manager *Manager, reader *kafka.Reader) error {
	return ConsumeAcksWith(ctx, manager, reader, relayclient.New())
}

// ConsumeAcksWith 注入 Relayer，便于单测替换 gRPC。
func ConsumeAcksWith(ctx context.Context, manager *Manager, reader *kafka.Reader, relayer AckRelayer) error {
	topic := reader.Config().Topic
	ackLog.Infof("ack consumer started on %s (group=%s) [shared-batch]", topic, reader.Config().GroupID)
	d := &ackDispatcher{manager: manager, relayer: relayer}
	for {
		m, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				ackLog.Info("ack consumer stopped")
				return nil
			}
			return fmt.Errorf("ack consume %s: %w", topic, err)
		}
		batch := []kafka.Message{m}
		dctx, dcancel := context.WithTimeout(ctx, ackBatchWait)
		for len(batch) < ackBatchMax {
			m2, derr := reader.FetchMessage(dctx)
			if derr != nil {
				break
			}
			batch = append(batch, m2)
		}
		dcancel()
		acks := make([]chat.Ack, 0, len(batch))
		for _, bm := range batch {
			var ack chat.Ack
			if err := json.Unmarshal(bm.Value, &ack); err != nil {
				ackLog.Errorf("ack bad payload (partition=%d offset=%d): %v", bm.Partition, bm.Offset, err)
				continue
			}
			acks = append(acks, ack)
		}
		if err := d.dispatch(ctx, acks); err != nil {
			return err
		}
		if err := reader.CommitMessages(ctx, batch...); err != nil {
			return fmt.Errorf("ack commit: %w", err)
		}
	}
}

type ackDispatcher struct {
	manager *Manager
	relayer AckRelayer
}

func (d *ackDispatcher) dispatch(ctx context.Context, acks []chat.Ack) error {
	if len(acks) == 0 {
		return nil
	}
	if d.manager.online == nil {
		for i := range acks {
			d.enqueueLocal(&acks[i])
		}
		return nil
	}
	uids := uniqueFromUIDs(acks)
	owners, err := d.manager.online.GetMany(ctx, uids)
	if err != nil {
		return fmt.Errorf("ack online lookup: %w", err)
	}
	localAddr := d.manager.OnlineAddr()
	remote := make(map[string][]chat.Ack)
	for i := range acks {
		ack := acks[i]
		owner, ok := owners[ack.FromUID]
		if !ok || owner.Addr == "" {
			continue
		}
		if owner.Addr == localAddr {
			d.enqueueLocal(&ack)
			continue
		}
		remote[owner.Addr] = append(remote[owner.Addr], ack)
	}
	for addr, list := range remote {
		if err := d.forward(ctx, addr, list); err != nil {
			return err
		}
	}
	return nil
}

func (d *ackDispatcher) enqueueLocal(ack *chat.Ack) {
	client := d.manager.GetClient(ack.FromUID)
	if client == nil {
		return
	}
	raw, err := json.Marshal(AckFrame{Cmd: "ack", Data: *ack})
	if err != nil {
		ackLog.Errorf("ack marshal %s: %v", ack.MsgID, err)
		return
	}
	select {
	case client.Send <- raw:
	default:
		ackLog.Warnf("ack %s dropped: send chan full (from=%s)", ack.MsgID, ack.FromUID)
	}
}

func (d *ackDispatcher) forward(ctx context.Context, addr string, acks []chat.Ack) error {
	if d.relayer == nil {
		return nil
	}
	req := &jokerproto.BatchRelayAcksRequest{Acks: make([]*jokerproto.RelayAckRequest, 0, len(acks))}
	for i := range acks {
		ack := acks[i]
		req.Acks = append(req.Acks, &jokerproto.RelayAckRequest{
			MsgId: ack.MsgID, CliMsgId: ack.CliMsgID, ConvId: ack.ConvID,
			FromUid: ack.FromUID, ToUid: ack.ToUID, Seq: ack.Seq,
		})
	}
	resp, err := d.relayer.BatchRelayAcks(ctx, addr, req)
	if err != nil {
		return fmt.Errorf("ack relay to %s: %w", addr, err)
	}
	if resp == nil {
		return nil
	}
	needRetry := make([]string, 0)
	for i, result := range resp.Results {
		if result != nil && !result.Delivered && result.Reason == "not_found" && i < len(acks) {
			needRetry = append(needRetry, acks[i].FromUID)
		}
	}
	if len(needRetry) == 0 {
		return nil
	}
	owners, err := d.manager.online.GetMany(ctx, uniqueStrings(needRetry))
	if err != nil {
		return err
	}
	retryByAddr := make(map[string][]chat.Ack)
	for i, result := range resp.Results {
		if result == nil || result.Delivered || result.Reason != "not_found" || i >= len(acks) {
			continue
		}
		ack := acks[i]
		owner, ok := owners[ack.FromUID]
		if !ok || owner.Addr == "" {
			continue
		}
		if owner.Addr == d.manager.OnlineAddr() {
			d.enqueueLocal(&ack)
			continue
		}
		retryByAddr[owner.Addr] = append(retryByAddr[owner.Addr], ack)
	}
	for retryAddr, list := range retryByAddr {
		retryReq := &jokerproto.BatchRelayAcksRequest{Acks: make([]*jokerproto.RelayAckRequest, 0, len(list))}
		for i := range list {
			ack := list[i]
			retryReq.Acks = append(retryReq.Acks, &jokerproto.RelayAckRequest{
				MsgId: ack.MsgID, CliMsgId: ack.CliMsgID, ConvId: ack.ConvID,
				FromUid: ack.FromUID, ToUid: ack.ToUID, Seq: ack.Seq,
			})
		}
		if _, err := d.relayer.BatchRelayAcks(ctx, retryAddr, retryReq); err != nil {
			ackLog.Warnf("ack retry relay to %s failed: %v", retryAddr, err)
		}
	}
	return nil
}

func uniqueFromUIDs(acks []chat.Ack) []string {
	seen := make(map[string]struct{}, len(acks))
	out := make([]string, 0, len(acks))
	for i := range acks {
		uid := acks[i].FromUID
		if _, ok := seen[uid]; ok {
			continue
		}
		seen[uid] = struct{}{}
		out = append(out, uid)
	}
	return out
}

func uniqueStrings(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, s := range in {
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}
