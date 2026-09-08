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
	"time"
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

	// S3c 批量化：批处理期间 producePush 只入队不写，批尾一次 WriteBatch——
	// 单条路径的 acks=all 跨机确认 ~8ms/条是 S3 实测主税（205 TPS 墙）。
	// 仅 Run 单协程访问，无需加锁；collecting=false 时（如测试直调 Handle）保持逐条写。
	collecting bool
	pending    []kafka.Message
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
// Run 消费主循环（S3c 批量化）：批拉 → 逐条 Handle（推送入队）→ 一次批 produce → 一次批 commit。
// 摊薄两类固定税：acks=all 跨机确认（produce）与组提交往返（commit）——S3 实测单条串行
// 路径 ~20-60ms/条把系统钉在 205 TPS（12 消费者扩容证伪），批量化是除以 N 的解法。
func (w *Worker) Run(ctx context.Context) error {
	log.Infof("persist: consuming %s (group=%s) [batch]", chat.TopicMsg, w.reader.Config().GroupID)
	const (
		batchMax  = 128
		batchWait = 200 * time.Millisecond
	)
	for {
		m, err := w.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				log.Info("persist: consumer stopped")
				return nil
			}
			return fmt.Errorf("persist: fetch: %w", err)
		}
		batch := []kafka.Message{m}
		dctx, dcancel := context.WithTimeout(ctx, batchWait)
		for len(batch) < batchMax {
			m2, derr := w.reader.FetchMessage(dctx)
			if derr != nil {
				break // 200ms 攒批窗口到：低流量时小批也出批
			}
			batch = append(batch, m2)
		}
		dcancel()

		// 解析整批；坏帧标记跳过（随批提交，与旧版 commit-skip 等价）
		w.collecting = true
		items := make([]*chat.Msg, len(batch))
		var singles []*chat.Msg
		for i, bm := range batch {
			var msg chat.Msg
			if err := json.Unmarshal(bm.Value, &msg); err != nil {
				log.Errorf("persist: bad payload (partition=%d offset=%d): %v", bm.Partition, bm.Offset, err)
				continue
			}
			if msg.Type == "" {
				msg.Type = chat.TypeSingle
			}
			items[i] = &msg
			if msg.Type == chat.TypeSingle {
				singles = append(singles, &msg)
			}
		}

		// S3c L2：单聊热路径整批落库（5 句查询/批）；前置不满足回退逐条（poison 语义精确）。
		// 非单聊（群/系统事件）维持逐条 Handle。
		var fail error
		if err := w.handleSingleBatch(ctx, singles); err != nil {
			if errors.Is(err, ErrBatchFallback) {
				for _, m := range singles {
					if err := w.Handle(ctx, m); err != nil {
						if errors.Is(err, ErrPoison) {
							log.Warnf("persist: skip poison msg cli_msg_id=%s from=%s to=%s: %v",
								m.CliMsgID, m.FromUID, m.ToUID, err)
							continue
						}
						fail = err
						break
					}
				}
			} else {
				fail = err
			}
		}
		if fail == nil {
			for _, m := range items {
				if m == nil || m.Type == chat.TypeSingle {
					continue
				}
				if err := w.Handle(ctx, m); err != nil {
					if errors.Is(err, ErrPoison) {
						log.Warnf("persist: skip poison (non-single) cli_msg_id=%s: %v", m.CliMsgID, err)
						continue
					}
					fail = err
					break
				}
			}
		}
		if err := w.flushPushes(ctx); err != nil {
			w.collecting = false
			return fmt.Errorf("persist: flush pushes: %w", err)
		}
		w.collecting = false
		if fail != nil {
			// 整批不提交、返回等重投（粒度比旧版逐条粗，正确性由 msg_id 幂等兜底：
			// 已入库部分重放时走 persistTx 的 replay 分支，推送 at-least-once）
			return fmt.Errorf("persist: handle (redelivery expected): %w", fail)
		}
		if err := w.reader.CommitMessages(ctx, batch...); err != nil {
			return fmt.Errorf("persist: commit: %w", err)
		}
	}
}

// ErrBatchFallback 批路径前置校验不满足（用户/会话缺失、无 conv_id 首条消息等），
// 调用方退回逐条 Handle 以保留 poison 精确报错与 EnsureSingle 兜底。
var ErrBatchFallback = errors.New("persist: batch precondition failed")

// handleSingleBatch 单聊批量落库（S3c L2）：把逐条 ~14 次 PG 往返（L1 后 PG CPU 满载，
// 594 TPS @ 8300 QPS 打满 4 核）合并为整批 5 句：
// uid ANY 校验 / conv ANY 校验 / 事务内 seq 批自增（unnest UPSERT 一次返回各会话 next）/
// 消息批插（unnest，msg_id 冲突 DO NOTHING=重放）/ last_seq 批更新。
// 语义对齐 persistTx：同批同会话按批内顺序严格递增；seq 与消息同事务（回滚一致）。
func (w *Worker) handleSingleBatch(ctx context.Context, msgs []*chat.Msg) error {
	if len(msgs) == 0 {
		return nil
	}
	// S3c L2b：conv_id 为空是真实客户端常态（不预知会话号）——先按成员对批量解析：
	// single_key ANY 一次查出绝大多数已存在会话；查不到的（首条消息）逐条 EnsureSingle
	// 创建兜底（稳态下占比极小；smoke/新对话场景走慢路径不影响正确性）。
	var unresolved []*chat.Msg
	skNeed := make(map[string]struct{})
	for _, m := range msgs {
		if m.ConvID == "" {
			skNeed[database.SingleKey(m.FromUID, m.ToUID)] = struct{}{}
		}
	}
	if len(skNeed) > 0 {
		sks := make([]string, 0, len(skNeed))
		for sk := range skNeed {
			sks = append(sks, sk)
		}
		rows, err := w.pool.Pool().Query(ctx,
			`SELECT single_key, conv_id FROM conversations WHERE single_key = ANY($1)`, sks)
		if err != nil {
			return err
		}
		sk2conv := make(map[string]string, len(sks))
		for rows.Next() {
			var sk, cid string
			if err := rows.Scan(&sk, &cid); err != nil {
				rows.Close()
				return err
			}
			sk2conv[sk] = cid
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		resolved := make([]*chat.Msg, 0, len(msgs))
		for _, m := range msgs {
			if m.ConvID != "" {
				resolved = append(resolved, m)
				continue
			}
			if cid, ok := sk2conv[database.SingleKey(m.FromUID, m.ToUID)]; ok {
				m.ConvID = cid
				resolved = append(resolved, m)
			} else {
				unresolved = append(unresolved, m) // 首条消息：会话尚不存在
			}
		}
		msgs = resolved
	}
	for _, m := range unresolved {
		if err := w.Handle(ctx, m); err != nil {
			if errors.Is(err, ErrPoison) {
				log.Warnf("persist: skip poison (first-msg) cli_msg_id=%s: %v", m.CliMsgID, err)
				continue
			}
			return err
		}
	}
	if len(msgs) == 0 {
		return nil
	}

	// Kafka acks=all 超时重试允许同一帧重复到达。批路径必须在烧 seq 前按
	// 确定性 msg_id 消重：旧实现把重复帧也计入 delta，虽然 INSERT DO NOTHING
	// 保住了消息行，却留下 seq 空洞并重复扇出，S3c 高压时会虚增 push 流量。
	unique := make([]*chat.Msg, 0, len(msgs))
	seen := make(map[string]struct{}, len(msgs))
	for _, m := range msgs {
		if m.MsgID == "" {
			m.MsgID = deriveMsgID(m.ConvID, m.FromUID, m.CliMsgID)
		}
		if _, ok := seen[m.MsgID]; ok {
			continue
		}
		seen[m.MsgID] = struct{}{}
		unique = append(unique, m)
	}
	msgs = unique

	ids := make([]string, 0, len(msgs))
	for _, m := range msgs {
		ids = append(ids, m.MsgID)
	}
	rows, err := w.pool.Pool().Query(ctx,
		`SELECT msg_id, seq, content FROM messages WHERE msg_id = ANY($1)`, ids)
	if err != nil {
		return err
	}
	type replay struct {
		msg     *chat.Msg
		seq     int64
		content []byte
	}
	stored := make(map[string]replay, len(msgs))
	for rows.Next() {
		var id string
		var seq int64
		var content []byte
		if err := rows.Scan(&id, &seq, &content); err != nil {
			rows.Close()
			return err
		}
		stored[id] = replay{seq: seq, content: content}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	// 已落库重放复用原 seq 和原内容补发 push；只有真正的新消息进入
	// convCount/NextSeqTx，保证重试不会烧号。先暂存，等新增消息事务提交后
	// 一并入队，避免半批失败时把未成功事务的消息提前投递。
	replays := make([]replay, 0, len(stored))
	fresh := make([]*chat.Msg, 0, len(msgs))
	for _, m := range msgs {
		if old, ok := stored[m.MsgID]; ok {
			old.msg = m
			replays = append(replays, old)
			continue
		}
		fresh = append(fresh, m)
	}
	msgs = fresh
	if len(msgs) == 0 {
		for _, old := range replays {
			old.msg.Content = json.RawMessage(old.content)
			if err := w.producePush(ctx, old.msg, old.seq, old.msg.ToUID, false); err != nil {
				return err
			}
		}
		return nil
	}
	uidSet := make(map[string]struct{}, len(msgs)*2)
	convCount := make(map[string]int64, len(msgs))
	for _, m := range msgs {
		uidSet[m.FromUID] = struct{}{}
		uidSet[m.ToUID] = struct{}{}
		convCount[m.ConvID]++
	}
	uids := make([]string, 0, len(uidSet))
	for u := range uidSet {
		uids = append(uids, u)
	}
	var foundU int
	if err := w.pool.Pool().QueryRow(ctx,
		`SELECT count(*) FROM users WHERE uid = ANY($1)`, uids).Scan(&foundU); err != nil {
		return err
	}
	if foundU != len(uids) {
		return ErrBatchFallback // 含不存在用户：逐条路径给 poison 精确报错
	}
	convs := make([]string, 0, len(convCount))
	deltas := make([]int64, 0, len(convCount))
	for c, n := range convCount {
		convs = append(convs, c)
		deltas = append(deltas, n)
	}
	var foundC int
	if err := w.pool.Pool().QueryRow(ctx,
		`SELECT count(*) FROM conversations WHERE conv_id = ANY($1)`, convs).Scan(&foundC); err != nil {
		return err
	}
	if foundC != len(convs) {
		return ErrBatchFallback
	}

	tx, err := w.pool.Pool().Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// seq 批自增：一次 unnest UPSERT 拿到各会话自增后的 next（old+n）。
	// 注意 ON CONFLICT 子句只能引用目标表与 excluded（不能引用 CTE d）——
	// excluded.v 即本行尝试插入的 jsonb_build_object('next', n)，取回 delta。
	// key 必须拼 "seq:" 前缀：与单条路径 NextSeqTx 共用同一计数器，且 RETURNING 的
	// substring(k from 5) 依赖此前缀还原完整 conv_id（缺前缀会削掉 UUID 前 4 位，
	// 导致 nextAfter 查不中、seq 算出 0/负数——2026-09-08 S3c 23505 crash-loop 根因）。
	rows, err = tx.Query(ctx, `
		WITH d(k, n) AS (SELECT * FROM unnest($1::text[], $2::bigint[]))
		INSERT INTO kv (k, v)
		  SELECT 'seq:' || k, jsonb_build_object('next', n) FROM d
		  ON CONFLICT (k) DO UPDATE SET
		    v = jsonb_set(kv.v, '{next}', to_jsonb((kv.v->>'next')::bigint + (excluded.v->>'next')::bigint)),
		    version = kv.version + 1, updated_at = now()
		  RETURNING substring(k from 5), (v->>'next')::bigint`, convs, deltas)
	if err != nil {
		return fmt.Errorf("batch next seq: %w", err)
	}
	nextAfter := make(map[string]int64, len(convs))
	for rows.Next() {
		var convID string
		var n int64
		if err := rows.Scan(&convID, &n); err != nil {
			rows.Close()
			return err
		}
		nextAfter[convID] = n
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	// 按批内顺序分配 seq（同会话 old+1..old+n 严格递增），并组装批插数组
	assigned := make(map[string]int64, len(convCount))
	idArr := make([]string, len(msgs))
	cvArr := make([]string, len(msgs))
	sqArr := make([]int64, len(msgs))
	fuArr := make([]string, len(msgs))
	tyArr := make([]string, len(msgs))
	ctArr := make([]string, len(msgs))
	for i, m := range msgs {
		if m.MsgID == "" {
			m.MsgID = deriveMsgID(m.ConvID, m.FromUID, m.CliMsgID)
		}
		seq := nextAfter[m.ConvID] - convCount[m.ConvID] + assigned[m.ConvID] + 1
		assigned[m.ConvID]++
		raw, err := json.Marshal(m.Content)
		if err != nil {
			return fmt.Errorf("%w: content marshal: %v", ErrPoison, err)
		}
		idArr[i], cvArr[i], sqArr[i], fuArr[i], tyArr[i], ctArr[i] =
			m.MsgID, m.ConvID, seq, m.FromUID, m.Type, string(raw)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO messages (msg_id, conv_id, seq, from_uid, type, content)
		SELECT * FROM unnest($1::text[], $2::text[], $3::bigint[], $4::text[], $5::text[], $6::jsonb[])
		ON CONFLICT (msg_id) DO NOTHING`, idArr, cvArr, sqArr, fuArr, tyArr, ctArr); err != nil {
		// 带批内 seq 分布上下文：定位 uq_messages_conv_seq 冲突的具体会话
		var sample []string
		for i := 0; i < len(msgs) && len(sample) < 3; i++ {
			sample = append(sample, fmt.Sprintf("%s#%d", cvArr[i], sqArr[i]))
		}
		return fmt.Errorf("batch insert messages (conv#seq sample: %v, nextAfter=%v): %w",
			sample, nextAfter, err)
	}
	lastSeqs := make([]int64, len(convs))
	for i, c := range convs {
		lastSeqs[i] = nextAfter[c] // 本批该会话最大 seq（自增后的 next）
	}
	if _, err := tx.Exec(ctx, `
		UPDATE conversations c SET last_seq = d.s
		FROM (SELECT * FROM unnest($1::text[], $2::bigint[])) AS d(cid, s)
		WHERE c.conv_id = d.cid AND c.last_seq < d.s`, convs, lastSeqs); err != nil {
		return fmt.Errorf("batch update last_seq: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	// 落库成功 → 单聊 1:1 扇出入队（Run 的 flushPushes 统一写出）。新增消息用
	// 本批分配的 seq；已落库重放复用库内 seq/内容补投，保持 at-least-once 而不烧号。
	for i, m := range msgs {
		if err := w.producePush(ctx, m, sqArr[i], m.ToUID, false); err != nil {
			return err
		}
	}
	for _, old := range replays {
		old.msg.Content = json.RawMessage(old.content)
		if err := w.producePush(ctx, old.msg, old.seq, old.msg.ToUID, false); err != nil {
			return err
		}
	}
	return nil
}

// flushPushes 批尾冲刷：优先 Producer.WriteBatch（一次摊薄 acks=all 往返），
// 无批量能力的 PushProducer（集成测试 fake）退化为逐条 Write，语义不变。
func (w *Worker) flushPushes(ctx context.Context) error {
	if len(w.pending) == 0 {
		return nil
	}
	defer func() { w.pending = w.pending[:0] }()
	if bp, ok := w.push.(interface {
		WriteBatch(ctx context.Context, msgs []kafka.Message) error
	}); ok {
		if err := bp.WriteBatch(ctx, w.pending); err != nil {
			return fmt.Errorf("produce %s batch(%d): %w", chat.TopicPush, len(w.pending), err)
		}
		return nil
	}
	for _, p := range w.pending {
		if err := w.push.Write(ctx, string(p.Key), p.Value); err != nil {
			return fmt.Errorf("produce %s: %w", chat.TopicPush, err)
		}
	}
	return nil
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
	// S3c 批量化：批处理期间只入队，批尾 flushPushes 一次 WriteBatch
	if w.collecting {
		w.pending = append(w.pending, kafka.Message{Key: []byte(toUID), Value: raw})
		return nil
	}
	if err := w.push.Write(ctx, toUID, raw); err != nil {
		return fmt.Errorf("produce %s: %w", chat.TopicPush, err)
	}
	return nil
}
