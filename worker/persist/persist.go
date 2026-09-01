// Package persist 落库 worker（§3 步骤 3-6）：消费 chat.msg → 校验 →
// 事务{seq 自增 → INSERT messages → 更新 last_seq} → commit 后 produce chat.push。
//
// 红线（D19）：persist 不直接发起投递，落库后必须经 chat.push 二次入队。
// 纪律（D20）：实例内单 goroutine 消费、处理完才 commit——分区内串行的保守超集；
// 吞吐不足时的扩展路径 = 多实例（消费组分摊分区）→ 再谈实例内按分区并行。
package persist

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"alfred.brave.com/database"
	"alfred.brave.com/event"
	"alfred.brave.com/internal/chat"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/segmentio/kafka-go"
)

var log = event.Log

// msgIDNamespace 确定性 msg_id 的 uuid5 命名空间（项目自定，不与标准 NS 冲突）。
var msgIDNamespace = uuid.MustParse("9f1c8a2e-3b7d-4e5f-8a6c-2d9e1f0a3b5c")

// ErrPoison 毒消息：业务校验失败（对端不存在/非成员/会话不存在等）。
// 重投永远不会成功，log + 跳过 + commit（DLQ 记录为遗留项，见 T05 spec）。
var ErrPoison = errors.New("persist: poison message")

// PushProducer chat.push 生产者抽象（集成测试用 fake 替换）。
type PushProducer interface {
	Write(ctx context.Context, key string, value []byte) error
}

// Worker persist 消费者。
type Worker struct {
	convs  *database.ConversationStore
	users  *database.UserStore
	kv     *database.KvStore
	pool   *database.Store
	push   PushProducer
	reader *kafka.Reader
}

// New 构造 worker（brokers 用于消费 chat.msg；push 生产者注入）。
func New(store *database.Store, push PushProducer, brokers []string, groupID string) *Worker {
	return &Worker{
		convs: database.NewConversationStore(store),
		users: database.NewUserStore(store),
		kv:    database.NewKvStore(store),
		pool:  store,
		push:  push,
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers: brokers,
			GroupID: groupID,
			Topic:   chat.TopicMsg,
			// 默认 Range 均衡；MinFetch/MaxWait 取低延迟默认即可
		}),
	}
}

// Run 消费主循环：fetch → Handle → commit（at-least-once）。
// 基础设施错误（PG/Kafka 故障）返回错误导致进程退出，由部署层重启重投；
// 毒消息（业务校验失败）log + commit 跳过，避免无限重投。
func (w *Worker) Run(ctx context.Context) error {
	log.Infof("persist: consuming %s (group=%s)", chat.TopicMsg, w.reader.Config().GroupID)
	for {
		m, err := w.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				log.Info("persist: consumer stopped")
				return nil
			}
			return fmt.Errorf("persist: fetch: %w", err)
		}
		var msg chat.Msg
		if err := json.Unmarshal(m.Value, &msg); err != nil {
			log.Errorf("persist: bad payload (partition=%d offset=%d): %v", m.Partition, m.Offset, err)
			if err := w.reader.CommitMessages(ctx, m); err != nil {
				return fmt.Errorf("persist: commit bad payload: %w", err)
			}
			continue
		}
		if err := w.Handle(ctx, &msg); err != nil {
			if errors.Is(err, ErrPoison) {
				log.Warnf("persist: skip poison msg cli_msg_id=%s from=%s to=%s: %v",
					msg.CliMsgID, msg.FromUID, msg.ToUID, err)
				if err := w.reader.CommitMessages(ctx, m); err != nil {
					return fmt.Errorf("persist: commit poison: %w", err)
				}
				continue
			}
			return fmt.Errorf("persist: handle (redelivery expected): %w", err)
		}
		if err := w.reader.CommitMessages(ctx, m); err != nil {
			return fmt.Errorf("persist: commit: %w", err)
		}
	}
}

// deriveMsgID 确定性 msg_id：(conv, from, cli_msg_id) 派生 uuid5——重放/重投得到同一 ID，
// 配合 messages PK 幂等。cli_msg_id 缺失时退化为随机（客户端契约破坏，仅记录）。
func deriveMsgID(convID, fromUID, cliMsgID string) string {
	if cliMsgID == "" {
		return uuid.NewString()
	}
	return uuid.NewSHA1(msgIDNamespace, []byte(convID+"|"+fromUID+"|"+cliMsgID)).String()
}

// Handle 处理一条 chat.msg：校验 → 落库事务 → produce chat.push。
// 业务校验失败返回 ErrPoison；基础设施失败返回原错误（外层不 commit，重投）。
func (w *Worker) Handle(ctx context.Context, msg *chat.Msg) error {
	if msg.Type == "" {
		msg.Type = chat.TypeSingle
	}
	// 单聊校验：收发双方必须是注册用户（轻量点查；容量账内 120 TPS × 2 查无压力）
	if _, err := w.users.Get(ctx, msg.FromUID); err != nil {
		return fmt.Errorf("%w: from_uid %s not found", ErrPoison, msg.FromUID)
	}
	if _, err := w.users.Get(ctx, msg.ToUID); err != nil {
		return fmt.Errorf("%w: to_uid %s not found", ErrPoison, msg.ToUID)
	}

	// 会话解析：conv_id 空则按成员对兜底建会话（首条消息路径）
	var conv *database.Conversation
	var err error
	if msg.ConvID == "" {
		conv, err = w.convs.EnsureSingle(ctx, msg.FromUID, msg.ToUID)
		if err != nil {
			return fmt.Errorf("ensure conversation: %w", err)
		}
		msg.ConvID = conv.ConvID
	} else {
		conv, err = w.convs.Get(ctx, msg.ConvID)
		if errors.Is(err, database.ErrNotFound) {
			return fmt.Errorf("%w: conv %s not found", ErrPoison, msg.ConvID)
		}
		if err != nil {
			return err
		}
		// 发送者必须是会话成员（群聊成员/角色校验在 T14 扩展）
		ok, err := w.convs.IsMember(ctx, conv.ConvID, msg.FromUID)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("%w: from_uid %s not member of %s", ErrPoison, msg.FromUID, conv.ConvID)
		}
	}

	if msg.MsgID == "" {
		msg.MsgID = deriveMsgID(conv.ConvID, msg.FromUID, msg.CliMsgID)
	}

	// 落库事务：重放快查 → seq 自增 → INSERT → last_seq
	content, err := json.Marshal(msg.Content)
	if err != nil {
		return fmt.Errorf("%w: content marshal: %v", ErrPoison, err)
	}
	seq, created, err := w.persistTx(ctx, msg, content)
	if err != nil {
		return err
	}
	if !created {
		// 重放：行已存在（同 msg_id），跳过重复落库但仍补发 chat.push（at-least-once 投递侧幂等）
		log.Debugf("persist: replay msg %s (seq=%d)", msg.MsgID, seq)
	}
	return w.producePush(ctx, msg, seq)
}

// persistTx 落库事务。返回 (seq, 是否新插入)。重放时不烧 seq（无空洞）。
func (w *Worker) persistTx(ctx context.Context, msg *chat.Msg, content []byte) (int64, bool, error) {
	tx, err := w.pool.Pool().Begin(ctx)
	if err != nil {
		return 0, false, err
	}
	defer tx.Rollback(ctx)

	// 重放快查：确定性 msg_id 已存在则复用其 seq
	var seq int64
	err = tx.QueryRow(ctx, `SELECT seq FROM messages WHERE msg_id = $1`, msg.MsgID).Scan(&seq)
	switch {
	case err == nil:
		return seq, false, nil
	case errors.Is(err, pgx.ErrNoRows):
		// 新消息，走插入
	default:
		return 0, false, err
	}

	if seq, err = w.kv.NextSeqTx(ctx, tx, msg.ConvID); err != nil {
		return 0, false, err
	}
	if _, err = tx.Exec(ctx,
		`INSERT INTO messages (msg_id, conv_id, seq, from_uid, type, content)
		 VALUES ($1, $2, $3, $4, $5, $6::jsonb)`,
		msg.MsgID, msg.ConvID, seq, msg.FromUID, msg.Type, content); err != nil {
		// UNIQUE(conv_id,seq) 冲突 = 并发路径竞争（理论不可达：分区串行 + 行锁）；显式暴露
		return 0, false, fmt.Errorf("insert message conv=%s seq=%d: %w", msg.ConvID, seq, err)
	}
	if _, err = tx.Exec(ctx,
		`UPDATE conversations SET last_seq = $2 WHERE conv_id = $1 AND last_seq < $2`,
		msg.ConvID, seq); err != nil {
		return 0, false, err
	}
	if err = tx.Commit(ctx); err != nil {
		return 0, false, err
	}
	return seq, true, nil
}

// producePush 落库 commit 后二次入队（D19）。失败返回错误触发不 commit → 整体重投（幂等）。
func (w *Worker) producePush(ctx context.Context, msg *chat.Msg, seq int64) error {
	push := chat.Push{
		MsgID:    msg.MsgID,
		CliMsgID: msg.CliMsgID,
		ConvID:   msg.ConvID,
		Seq:      seq,
		FromUID:  msg.FromUID,
		ToUID:    msg.ToUID,
		Type:     msg.Type,
		Content:  msg.Content,
	}
	raw, err := json.Marshal(push)
	if err != nil {
		return err
	}
	if err := w.push.Write(ctx, push.ToUID, raw); err != nil {
		return fmt.Errorf("produce %s: %w", chat.TopicPush, err)
	}
	return nil
}
