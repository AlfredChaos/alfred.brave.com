package exchange

import (
	"context"
	"encoding/json"
	"fmt"

	"alfred.brave.com/event"
	"alfred.brave.com/internal/chat"

	"github.com/segmentio/kafka-go"
)

var ackLog = event.Log

// AckFrame cmd=ack 下行帧：送达回执推给发送者（§3 步骤 13-15）。
// 客户端按 cli_msg_id 把消息状态从“已受理”翻为“已送达”（打勾），按 msg_id 幂等。
type AckFrame struct {
	Cmd  string   `json:"cmd"`
	Data chat.Ack `json:"data"`
}

// ConsumeAcks 消费 chat.ack 并推给本机连接的发送者。
//
// 消费组语义：每个 CS 实例独立 group（"cs-ack-{ServiceId}"）——ack 需要广播到所有 CS，
// 由各自过滤本机连接；不在本机的 ack 直接 commit 跳过。
// 该消费者与 Manager 事件循环解耦：只读连接表（GetClient 线程安全），不参与生命周期。
func ConsumeAcks(ctx context.Context, manager *Manager, reader *kafka.Reader) error {
	topic := reader.Config().Topic
	ackLog.Infof("ack consumer started on %s (group=%s)", topic, reader.Config().GroupID)
	for {
		m, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				ackLog.Info("ack consumer stopped")
				return nil
			}
			return fmt.Errorf("ack consume %s: %w", topic, err)
		}
		var ack chat.Ack
		if err := json.Unmarshal(m.Value, &ack); err != nil {
			ackLog.Errorf("ack bad payload (partition=%d offset=%d): %v", m.Partition, m.Offset, err)
			if err := reader.CommitMessages(ctx, m); err != nil {
				return err
			}
			continue
		}
		if client := manager.GetClient(ack.FromUID); client != nil {
			raw, err := json.Marshal(AckFrame{Cmd: "ack", Data: ack})
			if err != nil {
				ackLog.Errorf("ack marshal %s: %v", ack.MsgID, err)
			} else {
				select {
				case client.Send <- raw:
				default:
					// Send 满丢弃 ack：客户端有 seq 补拉兜底（D15），ack 不是唯一正确性来源
					ackLog.Warnf("ack %s dropped: send chan full (from=%s)", ack.MsgID, ack.FromUID)
				}
			}
		}
		if err := reader.CommitMessages(ctx, m); err != nil {
			return err
		}
	}
}
