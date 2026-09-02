package database

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// Post 朋友圈帖子（表 posts）。content 为 JSONB（{text}），media 为媒体引用数组。
type Post struct {
	PostID    string    `json:"post_id"`
	UID       string    `json:"uid"`
	Content   []byte    `json:"content"`
	Media     []byte    `json:"media"`
	IsBigV    bool      `json:"is_big_v"`
	IsDeleted bool      `json:"is_deleted"`
	CreatedAt time.Time `json:"created_at"`
}

// InboxEntry 收件箱引用行。
type InboxEntry struct {
	UID       string    `json:"uid"`
	PostID    string    `json:"post_id"`
	CreatedAt time.Time `json:"created_at"`
}

// PostAction 点赞/评论记录（content 评论时为 {text}）。
type PostAction struct {
	PostID    string    `json:"post_id"`
	UID       string    `json:"uid"`
	Action    string    `json:"action"`
	Content   []byte    `json:"content"`
	CreatedAt time.Time `json:"created_at"`
}

// PostCounter 聚合计数。
type PostCounter struct {
	PostID     string    `json:"post_id"`
	LikeCnt    int64     `json:"like_cnt"`
	CommentCnt int64     `json:"comment_cnt"`
	UpdatedAt  time.Time `json:"updated_at"`
}

const (
	ActionLike    = "like"
	ActionComment = "comment"
)

type FeedStore struct {
	store *Store
}

func NewFeedStore(s *Store) *FeedStore {
	return &FeedStore{store: s}
}

// CreatePost 发布落库；post_id 服务端生成。isBigV 由调用方按好友数判定（>5000）。
func (fs *FeedStore) CreatePost(ctx context.Context, uid string, content, media []byte, isBigV bool) (*Post, error) {
	p := &Post{
		PostID:  uuid.NewString(),
		UID:     uid,
		Content: content,
		Media:   media,
		IsBigV:  isBigV,
	}
	err := fs.store.pool.QueryRow(ctx,
		`INSERT INTO posts (post_id, uid, content, media, is_big_v)
		 VALUES ($1, $2, $3::jsonb, $4::jsonb, $5)
		 RETURNING created_at`,
		p.PostID, uid, content, media, isBigV).Scan(&p.CreatedAt)
	if err != nil {
		return nil, err
	}
	return p, nil
}

// GetPosts 批量取帖（hydrate 用）；返回按入参顺序的 map 便于调用方组装。
func (fs *FeedStore) GetPosts(ctx context.Context, postIDs []string) (map[string]*Post, error) {
	result := make(map[string]*Post, len(postIDs))
	if len(postIDs) == 0 {
		return result, nil
	}
	rows, err := fs.store.pool.Query(ctx,
		`SELECT post_id, uid, content, media, is_big_v, is_deleted, created_at
		 FROM posts WHERE post_id = ANY($1)`, postIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		p := &Post{}
		if err := rows.Scan(&p.PostID, &p.UID, &p.Content, &p.Media, &p.IsBigV, &p.IsDeleted, &p.CreatedAt); err != nil {
			return nil, err
		}
		result[p.PostID] = p
	}
	return result, rows.Err()
}

// PullRecentByAuthors pull 读取：给定作者集合（big_v 好友 + 自己）的最近 N 条。
func (fs *FeedStore) PullRecentByAuthors(ctx context.Context, uids []string, limit int) ([]Post, error) {
	if len(uids) == 0 || limit <= 0 {
		return nil, nil
	}
	rows, err := fs.store.pool.Query(ctx,
		`SELECT post_id, uid, content, media, is_big_v, is_deleted, created_at
		 FROM posts WHERE uid = ANY($1) AND NOT is_deleted
		 ORDER BY created_at DESC LIMIT $2`, uids, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	posts := make([]Post, 0)
	for rows.Next() {
		var p Post
		if err := rows.Scan(&p.PostID, &p.UID, &p.Content, &p.Media, &p.IsBigV, &p.IsDeleted, &p.CreatedAt); err != nil {
			return nil, err
		}
		posts = append(posts, p)
	}
	return posts, rows.Err()
}

// InsertInbox fanout 批量写收件箱（COPY 语义用多值 INSERT，500/批由调用方切）。
func (fs *FeedStore) InsertInbox(ctx context.Context, uid string, postID string, createdAt time.Time) error {
	_, err := fs.store.pool.Exec(ctx,
		`INSERT INTO feed_inbox (uid, post_id, created_at) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`,
		uid, postID, createdAt)
	return err
}

// InsertInboxBatch 单批多行写入（一个语句完成，减少往返）。
func (fs *FeedStore) InsertInboxBatch(ctx context.Context, uids []string, postID string, createdAt time.Time) error {
	if len(uids) == 0 {
		return nil
	}
	_, err := fs.store.pool.Exec(ctx,
		`INSERT INTO feed_inbox (uid, post_id, created_at)
		 SELECT uid, $2::varchar, $3 FROM unnest($1::varchar[]) AS uid
		 ON CONFLICT DO NOTHING`,
		uids, postID, createdAt)
	return err
}

// PageInbox 收件箱游标分页：beforeCreatedAt 之前的一页（倒序）。
func (fs *FeedStore) PageInbox(ctx context.Context, uid string, before time.Time, limit int) ([]InboxEntry, error) {
	if limit <= 0 || limit > 50 {
		limit = 20
	}
	rows, err := fs.store.pool.Query(ctx,
		`SELECT uid, post_id, created_at FROM feed_inbox
		 WHERE uid = $1 AND created_at < $2
		 ORDER BY created_at DESC LIMIT $3`, uid, before, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	entries := make([]InboxEntry, 0)
	for rows.Next() {
		var e InboxEntry
		if err := rows.Scan(&e.UID, &e.PostID, &e.CreatedAt); err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
	return entries, rows.Err()
}

// GetCounters 批量取计数（hydrate 用）。
func (fs *FeedStore) GetCounters(ctx context.Context, postIDs []string) (map[string]*PostCounter, error) {
	result := make(map[string]*PostCounter, len(postIDs))
	if len(postIDs) == 0 {
		return result, nil
	}
	rows, err := fs.store.pool.Query(ctx,
		`SELECT post_id, like_cnt, comment_cnt, updated_at FROM post_counters WHERE post_id = ANY($1)`, postIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var c PostCounter
		if err := rows.Scan(&c.PostID, &c.LikeCnt, &c.CommentCnt, &c.UpdatedAt); err != nil {
			return nil, err
		}
		result[c.PostID] = &c
	}
	return result, rows.Err()
}

// SoftDeletePost 帖子 tombstone（读取时过滤，inbox 不物理清理）。
func (fs *FeedStore) SoftDeletePost(ctx context.Context, postID string) error {
	_, err := fs.store.pool.Exec(ctx, `UPDATE posts SET is_deleted = true WHERE post_id = $1`, postID)
	return err
}

// CountFriendsByOwner 好友数（is_big_v 判定用；D07 双边行，owner 侧计数即好友数）。
func (fs *FeedStore) CountFriendsByOwner(ctx context.Context, uid string) (int, error) {
	var n int
	err := fs.store.pool.QueryRow(ctx,
		`SELECT count(*) FROM friends WHERE owner_uid = $1`, uid).Scan(&n)
	return n, err
}
