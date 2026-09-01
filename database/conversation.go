package database

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// SingleKey 单聊会话的规范化成员对 key：uid 字典序小者在前，与参数顺序无关。
// 唯一约束 uq_conversations_single_key 依赖该规范化防止 A→B / B→A 重复建会话。
func SingleKey(a, b string) string {
	if a > b {
		a, b = b, a
	}
	return a + ":" + b
}

// Conversation 会话（表 conversations）。单聊与群聊共用；群聊 conv_id 即 gid。
type Conversation struct {
	ConvID    string          `json:"conv_id"`
	Type      string          `json:"type"` // single|group
	SingleKey *string         `json:"single_key,omitempty"`
	Members   json.RawMessage `json:"members"`
	LastSeq   int64           `json:"last_seq"`
	CreatedAt time.Time       `json:"created_at"`
}

// ErrDuplicate 单聊会话已存在（由唯一约束触发）——调用方据此改为查询复用。
var ErrDuplicate = errors.New("duplicate record")

type ConversationStore struct {
	store *Store
}

func NewConversationStore(s *Store) *ConversationStore {
	return &ConversationStore{store: s}
}

// CreateSingle 建立单聊会话；single_key 冲突（会话已存在）返回 ErrDuplicate。
func (cs *ConversationStore) CreateSingle(ctx context.Context, uidA, uidB string) (*Conversation, error) {
	convID := uuid.NewString()
	sk := SingleKey(uidA, uidB)
	members, err := json.Marshal([]string{uidA, uidB})
	if err != nil {
		return nil, err
	}
	_, err = cs.store.pool.Exec(ctx,
		`INSERT INTO conversations (conv_id, type, single_key, members) VALUES ($1, 'single', $2, $3::jsonb)`,
		convID, sk, members)
	if err != nil {
		if isUniqueViolation(err) {
			return nil, ErrDuplicate
		}
		return nil, err
	}
	return cs.Get(ctx, convID)
}

// GetBySingleKey 按成员对查会话。
func (cs *ConversationStore) GetBySingleKey(ctx context.Context, uidA, uidB string) (*Conversation, error) {
	conv := &Conversation{}
	err := cs.store.pool.QueryRow(ctx,
		`SELECT conv_id, type, single_key, members, last_seq, created_at FROM conversations WHERE single_key = $1`,
		SingleKey(uidA, uidB)).Scan(&conv.ConvID, &conv.Type, &conv.SingleKey, &conv.Members, &conv.LastSeq, &conv.CreatedAt)
	if err != nil {
		return nil, wrapNoRows(err)
	}
	return conv, nil
}

// Get 按会话 ID 查。
func (cs *ConversationStore) Get(ctx context.Context, convID string) (*Conversation, error) {
	conv := &Conversation{}
	err := cs.store.pool.QueryRow(ctx,
		`SELECT conv_id, type, single_key, members, last_seq, created_at FROM conversations WHERE conv_id = $1`,
		convID).Scan(&conv.ConvID, &conv.Type, &conv.SingleKey, &conv.Members, &conv.LastSeq, &conv.CreatedAt)
	if err != nil {
		return nil, wrapNoRows(err)
	}
	return conv, nil
}

// EnsureSingle find-or-create 单聊会话（事务内）：按 single_key 查，无则建会话 + 双方成员行。
// 网关 API（POST /v1/conversations）与 persist（首条消息兜底）共用，天然幂等。
func (cs *ConversationStore) EnsureSingle(ctx context.Context, uidA, uidB string) (*Conversation, error) {
	if uidA == uidB {
		return nil, ErrNotFound
	}
	tx, err := cs.store.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	sk := SingleKey(uidA, uidB)
	var convID string
	err = tx.QueryRow(ctx,
		`SELECT conv_id FROM conversations WHERE single_key = $1 FOR UPDATE`, sk).Scan(&convID)
	if err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			return nil, err
		}
		// 不存在则创建（FOR UPDATE 空结果不持锁；唯一约束兜底并发竞争）
		convID = uuid.NewString()
		members, merr := json.Marshal([]string{uidA, uidB})
		if merr != nil {
			return nil, merr
		}
		if _, err = tx.Exec(ctx,
			`INSERT INTO conversations (conv_id, type, single_key, members) VALUES ($1, 'single', $2, $3::jsonb)`,
			convID, sk, members); err != nil {
			return nil, err
		}
		for _, uid := range []string{uidA, uidB} {
			if _, err = tx.Exec(ctx,
				`INSERT INTO conversation_members (conv_id, uid) VALUES ($1, $2)`, convID, uid); err != nil {
				return nil, err
			}
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	return cs.Get(ctx, convID)
}

// AddMembers 写会话成员表（幂等）。
func (cs *ConversationStore) AddMembers(ctx context.Context, convID string, uids []string) error {
	tx, err := cs.store.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	for _, uid := range uids {
		if _, err := tx.Exec(ctx,
			`INSERT INTO conversation_members (conv_id, uid) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
			convID, uid); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// isUniqueViolation 判定 PG 唯一约束冲突（23505）。
func isUniqueViolation(err error) bool {
	var pgErr interface{ SQLState() string }
	if errors.As(err, &pgErr) {
		return pgErr.SQLState() == "23505"
	}
	return false
}
