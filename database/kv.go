package database

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// KvStore kv 表数据访问（D05：PG 兼作 KV）。
// 用途：online:{uid} 在线路由（D06）、seq:{conv_id} 会话序号（D08/D16）、feedcur:{uid} 读游标。
// 并发说明：单条语句原子（UPSERT/条件 DELETE），无进程内共享状态。
type KvStore struct {
	store *Store
}

func NewKvStore(s *Store) *KvStore {
	return &KvStore{store: s}
}

// Put upsert 整个 value；version 自增（乐观并发痕迹，当前无读者，留作演进）。
func (ks *KvStore) Put(ctx context.Context, key string, value interface{}) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("kv put marshal %s: %w", key, err)
	}
	_, err = ks.store.pool.Exec(ctx,
		`INSERT INTO kv (k, v) VALUES ($1, $2)
		 ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v, version = kv.version + 1, updated_at = now()`,
		key, raw)
	return err
}

// Get 读取 value 并反序列化到 out；不存在返回 ErrNotFound。
func (ks *KvStore) Get(ctx context.Context, key string, out interface{}) error {
	var raw []byte
	err := ks.store.pool.QueryRow(ctx, `SELECT v FROM kv WHERE k = $1`, key).Scan(&raw)
	if err != nil {
		return wrapNoRows(err)
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("kv get unmarshal %s: %w", key, err)
	}
	return nil
}

// GetManyBatch 批量读取 kv（群聊扇出前批量查在线成员）。返回 key → raw value。
// key 形如 online:{uid}；返回 map 不含缺失键。
func (ks *KvStore) GetManyBatch(ctx context.Context, keys []string) (map[string]json.RawMessage, error) {
	result := make(map[string]json.RawMessage, len(keys))
	if len(keys) == 0 {
		return result, nil
	}
	rows, err := ks.store.pool.Query(ctx, `SELECT k, v FROM kv WHERE k = ANY($1)`, keys)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var k string
		var v json.RawMessage
		if err := rows.Scan(&k, &v); err != nil {
			return nil, err
		}
		result[k] = v
	}
	return result, rows.Err()
}

// Delete 无条件删除（键级幂等）。
func (ks *KvStore) Delete(ctx context.Context, key string) error {
	_, err := ks.store.pool.Exec(ctx, `DELETE FROM kv WHERE k = $1`, key)
	return err
}

// DeleteIfMatch 条件删除：仅当 v->>'cs' 等于 self 时删除。
// 用于 WS 断开清理 online:{uid}——重连漂移场景（用户已切到新 CS）旧节点的晚到 del 不误删新记录（D06/§4）。
func (ks *KvStore) DeleteIfMatch(ctx context.Context, key, field, self string) error {
	_, err := ks.store.pool.Exec(ctx,
		`DELETE FROM kv WHERE k = $1 AND v->>$2 = $3`, key, field, self)
	return err
}

// KvEntry 前缀扫描结果。
type KvEntry struct {
	Key       string          `json:"k"`
	Value     json.RawMessage `json:"v"`
	UpdatedAt time.Time       `json:"updated_at"`
}

// ScanPrefix 前缀扫描（GHOST 对账用 online:*）。
func (ks *KvStore) ScanPrefix(ctx context.Context, prefix string, limit int) ([]KvEntry, error) {
	if limit <= 0 {
		limit = 1000
	}
	rows, err := ks.store.pool.Query(ctx,
		`SELECT k, v, updated_at FROM kv WHERE k LIKE $1 || '%' ORDER BY k LIMIT $2`, prefix, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	entries := make([]KvEntry, 0)
	for rows.Next() {
		var e KvEntry
		if err := rows.Scan(&e.Key, &e.Value, &e.UpdatedAt); err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
	return entries, rows.Err()
}

// NextSeqTx 在给定事务内自增会话序号（persist 落库事务专用：seq 与 INSERT 同事务，
// 这是 D08"PG kv 事务内自增"的落地）。初始值必须为 1（无冲突分支直接返回插入值）。
func (ks *KvStore) NextSeqTx(ctx context.Context, tx pgx.Tx, convID string) (int64, error) {
	var next int64
	err := tx.QueryRow(ctx,
		`INSERT INTO kv (k, v) VALUES ($1, '{"next":1}'::jsonb)
		 ON CONFLICT (k) DO UPDATE
		   SET v = jsonb_set(kv.v, '{next}', to_jsonb((kv.v->>'next')::bigint + 1)),
		       version = kv.version + 1, updated_at = now()
		 RETURNING (v->>'next')::bigint`,
		"seq:"+convID).Scan(&next)
	if err != nil {
		return 0, fmt.Errorf("kv next seq tx %s: %w", convID, err)
	}
	return next, nil
}

// NextSeq 会话序号原子自增：首条消息得 1，之后严格递增。
// 单条 UPSERT 完成“初始化 + 自增 + 返回”，行级原子，persist 分区单 goroutine 消费下无竞争（D20）。
// 注意 INSERT 初始值必须是 1（无冲突分支直接返回插入行的值）。
func (ks *KvStore) NextSeq(ctx context.Context, convID string) (int64, error) {
	var next int64
	err := ks.store.pool.QueryRow(ctx,
		`INSERT INTO kv (k, v) VALUES ($1, '{"next":1}'::jsonb)
		 ON CONFLICT (k) DO UPDATE
		   SET v = jsonb_set(kv.v, '{next}', to_jsonb((kv.v->>'next')::bigint + 1)),
		       version = kv.version + 1, updated_at = now()
		 RETURNING (v->>'next')::bigint`,
		"seq:"+convID).Scan(&next)
	if err != nil {
		return 0, fmt.Errorf("kv next seq %s: %w", convID, err)
	}
	return next, nil
}
