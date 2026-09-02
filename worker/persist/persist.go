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
	groups *database.GroupStore
	kv     *database.KvStore
	pool   *database.Store
	push   PushProducer
	reader *kafka.Reader
}

// New 构造 worker（brokers 用于消费 chat.msg；push 生产者注入）。
func New(store *database.Store, push PushProducer, brokers []string, groupID string) *Worker {
	return &Worker{
		convs:  database.NewConversationStore(store),
		users:  database.NewUserStore(store),
		groups: database.NewGroupStore(store),
		kv:     database.NewKvStore(store),
		pool:   store,
		push:   push,
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

// IsPoison 判定错误是否毒消息（供测试与外层判断）。
func IsPoison(err error) bool { return errors.Is(err, ErrPoison) }

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
// 三类消息：
//   - single：单聊（to_uid 必填，会话按需兜底创建）
//   - group：群聊（conv_id=gid；权威校验群状态/成员身份；扇出=仅在线成员）
//   - system_event：群管理事件（网关已预写消息行，这里幂等跳过落库并统一扇出）
func (w *Worker) Handle(ctx context.Context, msg *chat.Msg) error {
	if msg.Type == "" {
		msg.Type = chat.TypeSingle
	}

	var targets []pushTarget
	switch msg.Type {
	case chat.TypeSystemEvent:
		// 预写消息：必须带 msg_id 与 conv_id（gid）。权威校验在网关写事务已完成；
		// 这里只解析扇出目标。注意解散事件的信封在群已 dismissed 后到达——
		// 不能做状态校验（否则末条消息永远投不出去），成员以 group_members 行为准。
		if msg.MsgID == "" || msg.ConvID == "" {
			return fmt.Errorf("%w: system_event requires msg_id and conv_id", ErrPoison)
		}
		members, err := w.groups.ActiveMembers(ctx, msg.ConvID)
		if err != nil {
			return err
		}
		for _, m := range members {
			targets = append(targets, pushTarget{UID: m.UID, Role: m.Role})
		}
	case chat.TypeGroup:
		members, err := w.validateGroupMsg(ctx, msg)
		if err != nil {
			return err
		}
		targets = members
	case chat.TypeSingle:
		if err := w.validateSingleMsg(ctx, msg); err != nil {
			return err
		}
	default:
		return fmt.Errorf("%w: unknown type %s", ErrPoison, msg.Type)
	}

	if msg.MsgID == "" {
		msg.MsgID = deriveMsgID(msg.ConvID, msg.FromUID, msg.CliMsgID)
	}
	content, err := json.Marshal(msg.Content)
	if err != nil {
		return fmt.Errorf("%w: content marshal: %v", ErrPoison, err)
	}
	seq, stored, created, err := w.persistTx(ctx, msg, content)
	if err != nil {
		return err
	}
	if !created {
		// 重放/预写命中：行已存在（同 msg_id），用库内内容扇出（信封里的 content 可能是占位）
		log.Debugf("persist: replay msg %s (seq=%d)", msg.MsgID, seq)
		msg.Content = json.RawMessage(stored)
	}

	// 扇出：单聊 1:1；群聊 1:M 仅在线成员（投递层写扩散 D08，存储恒 1 份）
	if msg.Type == chat.TypeSingle {
		return w.producePush(ctx, msg, seq, msg.ToUID, false)
	}
	online, err := w.onlineMembers(ctx, targets)
	if err != nil {
		return err
	}
	for _, m := range online {
		mention := msg.MentionAll || containsUID(msg.Mentions, m.UID)
		if err := w.producePush(ctx, msg, seq, m.UID, mention); err != nil {
			return err
		}
	}
	if len(online) == 0 {
		log.Debugf("persist: group %s msg %s fanout: no online member", msg.ConvID, msg.MsgID)
	}
	return nil
}

// pushTarget 扇出目标。
type pushTarget struct {
	UID  string
	Role string
}

// validateSingleMsg 单聊校验 + 会话解析（conv_id 空则按成员对兜底）。
func (w *Worker) validateSingleMsg(ctx context.Context, msg *chat.Msg) error {
	if _, err := w.users.Get(ctx, msg.FromUID); err != nil {
		return fmt.Errorf("%w: from_uid %s not found", ErrPoison, msg.FromUID)
	}
	if _, err := w.users.Get(ctx, msg.ToUID); err != nil {
		return fmt.Errorf("%w: to_uid %s not found", ErrPoison, msg.ToUID)
	}
	if msg.ConvID == "" {
		conv, err := w.convs.EnsureSingle(ctx, msg.FromUID, msg.ToUID)
		if err != nil {
			return fmt.Errorf("ensure conversation: %w", err)
		}
		msg.ConvID = conv.ConvID
		return nil
	}
	conv, err := w.convs.Get(ctx, msg.ConvID)
	if errors.Is(err, database.ErrNotFound) {
		return fmt.Errorf("%w: conv %s not found", ErrPoison, msg.ConvID)
	}
	if err != nil {
		return err
	}
	ok, err := w.convs.IsMember(ctx, conv.ConvID, msg.FromUID)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("%w: from_uid %s not member of %s", ErrPoison, msg.FromUID, conv.ConvID)
	}
	return nil
}

// validateGroupMsg 群聊权威校验（§5 persist 层，事务串行下原子判定）：
// 群存活 + 发送者有效成员 + @ 规则（mention_all 仅 owner；mentions ⊆ 当前成员）。
// 校验失败整条拒绝（不留半条消息）。CS 侧仅做格式快检（mentions≤50），此处才是权威。
func (w *Worker) validateGroupMsg(ctx context.Context, msg *chat.Msg) ([]pushTarget, error) {
	if msg.ConvID == "" {
		return nil, fmt.Errorf("%w: group msg requires conv_id(gid)", ErrPoison)
	}
	g, err := w.groups.GetGroup(ctx, msg.ConvID)
	if errors.Is(err, database.ErrNotFound) {
		return nil, fmt.Errorf("%w: group %s not found", ErrPoison, msg.ConvID)
	}
	if err != nil {
		return nil, err
	}
	if g.Status != database.GroupStatusActive {
		return nil, fmt.Errorf("%w: group %s dismissed", ErrPoison, msg.ConvID)
	}
	members, err := w.groups.ActiveMembers(ctx, msg.ConvID)
	if err != nil {
		return nil, err
	}
	senderRole := ""
	active := make(map[string]bool, len(members))
	targets := make([]pushTarget, 0, len(members))
	for _, m := range members {
		if m.UID == msg.FromUID {
			senderRole = m.Role
		}
		active[m.UID] = true
		targets = append(targets, pushTarget{UID: m.UID, Role: m.Role})
	}
	if senderRole == "" {
		return nil, fmt.Errorf("%w: sender %s not active member of %s", ErrPoison, msg.FromUID, msg.ConvID)
	}
	// @所有人：仅群主（§5 约束⑥）
	if msg.MentionAll && senderRole != database.GroupRoleOwner {
		return nil, fmt.Errorf("%w: mention_all requires owner (sender=%s role=%s)", ErrPoison, msg.FromUID, senderRole)
	}
	// @个人：每个 uid 必须是当前有效成员
	for _, uid := range msg.Mentions {
		if !active[uid] {
			return nil, fmt.Errorf("%w: mention %s not active member of %s", ErrPoison, uid, msg.ConvID)
		}
	}
	return targets, nil
}

// onlineMembers 批量查在线成员（投递写扩散只推在线；离线上线按 seq 补拉，§5 约束②）。
func (w *Worker) onlineMembers(ctx context.Context, targets []pushTarget) ([]pushTarget, error) {
	keys := make([]string, 0, len(targets))
	for _, t := range targets {
		keys = append(keys, "online:"+t.UID)
	}
	kvs, err := w.kv.GetManyBatch(ctx, keys)
	if err != nil {
		return nil, err
	}
	online := make([]pushTarget, 0, len(targets))
	for _, t := range targets {
		if _, ok := kvs["online:"+t.UID]; ok {
			online = append(online, t)
		}
	}
	return online, nil
}

func containsUID(list []string, uid string) bool {
	for _, v := range list {
		if v == uid {
			return true
		}
	}
	return false
}

// persistTx 落库事务。返回 (seq, 库内内容, 是否新插入)。重放/预写时不烧 seq（无空洞）。
func (w *Worker) persistTx(ctx context.Context, msg *chat.Msg, content []byte) (int64, []byte, bool, error) {
	tx, err := w.pool.Pool().Begin(ctx)
	if err != nil {
		return 0, nil, false, err
	}
	defer tx.Rollback(ctx)

	// 重放快查：确定性 msg_id 已存在则复用其 seq 与内容（system_event 预写同路径）
	var seq int64
	var stored []byte
	err = tx.QueryRow(ctx, `SELECT seq, content FROM messages WHERE msg_id = $1`, msg.MsgID).Scan(&seq, &stored)
	switch {
	case err == nil:
		return seq, stored, false, nil
	case errors.Is(err, pgx.ErrNoRows):
		// 新消息，走插入
	default:
		return 0, nil, false, err
	}

	if seq, err = w.kv.NextSeqTx(ctx, tx, msg.ConvID); err != nil {
		return 0, nil, false, err
	}
	if _, err = tx.Exec(ctx,
		`INSERT INTO messages (msg_id, conv_id, seq, from_uid, type, content)
		 VALUES ($1, $2, $3, $4, $5, $6::jsonb)`,
		msg.MsgID, msg.ConvID, seq, msg.FromUID, msg.Type, content); err != nil {
		// UNIQUE(conv_id,seq) 冲突 = 并发路径竞争（理论不可达：分区串行 + 行锁）；显式暴露
		return 0, nil, false, fmt.Errorf("insert message conv=%s seq=%d: %w", msg.ConvID, seq, err)
	}
	if _, err = tx.Exec(ctx,
		`UPDATE conversations SET last_seq = $2 WHERE conv_id = $1 AND last_seq < $2`,
		msg.ConvID, seq); err != nil {
		return 0, nil, false, err
	}
	if err = tx.Commit(ctx); err != nil {
		return 0, nil, false, err
	}
	return seq, content, true, nil
}

// producePush 落库 commit 后二次入队（D19）。toUID 为本条 push 的接收者
// （单聊恒 to_uid；群聊逐在线成员）。失败返回错误触发不 commit → 整体重投（幂等）。
func (w *Worker) producePush(ctx context.Context, msg *chat.Msg, seq int64, toUID string, mention bool) error {
	push := chat.Push{
		MsgID:    msg.MsgID,
		CliMsgID: msg.CliMsgID,
		ConvID:   msg.ConvID,
		Seq:      seq,
		FromUID:  msg.FromUID,
		ToUID:    toUID,
		Type:     msg.Type,
		Content:  msg.Content,
		Mention:  mention,
	}
	raw, err := json.Marshal(push)
	if err != nil {
		return err
	}
	if err := w.push.Write(ctx, toUID, raw); err != nil {
		return fmt.Errorf("produce %s: %w", chat.TopicPush, err)
	}
	return nil
}
