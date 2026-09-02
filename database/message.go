package database

import (
	"context"
	"encoding/json"
	"time"
)

// Message 消息（表 messages）。conv_id 单聊=会话ID，群聊=gid；seq 会话内严格递增。
type Message struct {
	MsgID     string          `json:"msg_id"`
	ConvID    string          `json:"conv_id"`
	Seq       int64           `json:"seq"`
	FromUID   string          `json:"from_uid"`
	Type      string          `json:"type"` // single|group|system_event
	Content   json.RawMessage `json:"content"`
	CreatedAt time.Time       `json:"created_at"`
}

type MessageStore struct {
	store *Store
}

func NewMessageStore(s *Store) *MessageStore {
	return &MessageStore{store: s}
}

// ListAfter 历史拉取：按 seq 游标升序分页（客户端补拉路径，D15 的服务端半边）。
func (ms *MessageStore) ListAfter(ctx context.Context, convID string, afterSeq int64, limit int) ([]Message, error) {
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	rows, err := ms.store.pool.Query(ctx,
		`SELECT msg_id, conv_id, seq, from_uid, type, content, created_at
		 FROM messages WHERE conv_id = $1 AND seq > $2 ORDER BY seq ASC LIMIT $3`,
		convID, afterSeq, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	msgs := make([]Message, 0)
	for rows.Next() {
		var m Message
		if err := rows.Scan(&m.MsgID, &m.ConvID, &m.Seq, &m.FromUID, &m.Type, &m.Content, &m.CreatedAt); err != nil {
			return nil, err
		}
		msgs = append(msgs, m)
	}
	return msgs, rows.Err()
}

// ListAfterForGroup 群历史拉取：按成员时间窗过滤（§5 约束⑦——
// created_at ∈ [joined_at, left_at)，新成员看不到加入前、被踢后不可见）。
func (ms *MessageStore) ListAfterForGroup(ctx context.Context, gid, uid string, afterSeq int64, limit int) ([]Message, error) {
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	rows, err := ms.store.pool.Query(ctx,
		`SELECT m.msg_id, m.conv_id, m.seq, m.from_uid, m.type, m.content, m.created_at
		 FROM messages m
		 JOIN group_members gm ON gm.gid = m.conv_id AND gm.uid = $2
		 WHERE m.conv_id = $1 AND m.seq > $3
		   AND m.created_at >= gm.joined_at
		   AND (gm.left_at IS NULL OR m.created_at <= gm.left_at)
		 ORDER BY m.seq ASC LIMIT $4`,
		gid, uid, afterSeq, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	msgs := make([]Message, 0)
	for rows.Next() {
		var m Message
		if err := rows.Scan(&m.MsgID, &m.ConvID, &m.Seq, &m.FromUID, &m.Type, &m.Content, &m.CreatedAt); err != nil {
			return nil, err
		}
		msgs = append(msgs, m)
	}
	return msgs, rows.Err()
}

// IsMember 判定 uid 是否会话成员（conversation_members 点查）。
func (cs *ConversationStore) IsMember(ctx context.Context, convID, uid string) (bool, error) {
	var exists bool
	err := cs.store.pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM conversation_members WHERE conv_id = $1 AND uid = $2)`,
		convID, uid).Scan(&exists)
	return exists, err
}

// ListByUser 某用户参与的会话列表（join 成员表，倒序）。
func (cs *ConversationStore) ListByUser(ctx context.Context, uid string) ([]Conversation, error) {
	rows, err := cs.store.pool.Query(ctx,
		`SELECT c.conv_id, c.type, c.single_key, c.members, c.last_seq, c.created_at
		 FROM conversations c
		 JOIN conversation_members m ON m.conv_id = c.conv_id
		 WHERE m.uid = $1 ORDER BY c.created_at DESC LIMIT 200`, uid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	convs := make([]Conversation, 0)
	for rows.Next() {
		var c Conversation
		if err := rows.Scan(&c.ConvID, &c.Type, &c.SingleKey, &c.Members, &c.LastSeq, &c.CreatedAt); err != nil {
			return nil, err
		}
		convs = append(convs, c)
	}
	return convs, rows.Err()
}
