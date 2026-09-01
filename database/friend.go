package database

import (
	"context"
)

// Friend 好友关系（表 friends）。D07：双边存两行——(A,B) 与 (B,A) 同时存在，
// 好友判定退化为 PK 点查，避免业务层 OR 查询。
type Friend struct {
	OwnerUID  string `json:"owner_uid"`
	FriendUID string `json:"friend_uid"`
	Status    int16  `json:"status"` // 1=正常
}

type FriendStore struct {
	store *Store
}

func NewFriendStore(s *Store) *FriendStore {
	return &FriendStore{store: s}
}

// Create 建立好友关系：单事务写双边两行，部分失败整体回滚（不允许出现单向好友）。
func (fs *FriendStore) Create(ctx context.Context, ownerUID, friendUID string) error {
	if ownerUID == friendUID {
		return ErrNotFound // 自加好友无意义，用 ErrNotFound 语义不够准确，但调用方走 4xx
	}
	tx, err := fs.store.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if _, err = tx.Exec(ctx,
		`INSERT INTO friends (owner_uid, friend_uid) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
		ownerUID, friendUID); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx,
		`INSERT INTO friends (owner_uid, friend_uid) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
		friendUID, ownerUID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// ListByOwner 某人的好友列表。
func (fs *FriendStore) ListByOwner(ctx context.Context, ownerUID string) ([]Friend, error) {
	rows, err := fs.store.pool.Query(ctx,
		`SELECT owner_uid, friend_uid, status FROM friends WHERE owner_uid = $1 ORDER BY created_at`, ownerUID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	friends := make([]Friend, 0)
	for rows.Next() {
		var f Friend
		if err := rows.Scan(&f.OwnerUID, &f.FriendUID, &f.Status); err != nil {
			return nil, err
		}
		friends = append(friends, f)
	}
	return friends, rows.Err()
}

// GetFriendship 点查两人是否为好友（任一方向存在即成立；正常态下双边对称）。
func (fs *FriendStore) GetFriendship(ctx context.Context, ownerUID, friendUID string) (*Friend, error) {
	f := &Friend{}
	err := fs.store.pool.QueryRow(ctx,
		`SELECT owner_uid, friend_uid, status FROM friends WHERE owner_uid = $1 AND friend_uid = $2`,
		ownerUID, friendUID).Scan(&f.OwnerUID, &f.FriendUID, &f.Status)
	if err != nil {
		return nil, wrapNoRows(err)
	}
	return f, nil
}

// Delete 删除好友：双边两行同事务删除。
func (fs *FriendStore) Delete(ctx context.Context, ownerUID, friendUID string) error {
	tx, err := fs.store.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if _, err = tx.Exec(ctx,
		`DELETE FROM friends WHERE owner_uid = $1 AND friend_uid = $2`, ownerUID, friendUID); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx,
		`DELETE FROM friends WHERE owner_uid = $1 AND friend_uid = $2`, friendUID, ownerUID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
