package api

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"alfred.brave.com/internal/abort"

	"github.com/gin-gonic/gin"
)

// FeedItem 读取响应的单条 timeline 项（hydrate 后）。
type FeedItem struct {
	PostID     string          `json:"post_id"`
	UID        string          `json:"uid"`
	AuthorName string          `json:"author_name"`
	Content    json.RawMessage `json:"content"`
	Media      json.RawMessage `json:"media"`
	LikeCnt    int64           `json:"like_cnt"`
	CommentCnt int64           `json:"comment_cnt"`
	CreatedAt  time.Time       `json:"created_at"`
}

// cursor 游标：createdAtNano + postID，base64url 不透明下发。
type cursor struct {
	ts     time.Time
	postID string
}

func encodeCursor(ts time.Time, postID string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(
		strconv.FormatInt(ts.UnixNano(), 10) + ":" + postID))
}

func decodeCursor(s string) (*cursor, error) {
	if s == "" {
		return nil, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return nil, err
	}
	parts := strings.SplitN(string(raw), ":", 2)
	if len(parts) != 2 {
		return nil, errBadCursor
	}
	nano, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return nil, err
	}
	return &cursor{ts: time.Unix(0, nano), postID: parts[1]}, nil
}

var errBadCursor = jsonError("invalid cursor")

type jsonError string

func (e jsonError) Error() string { return string(e) }

// timelineEntry merge 阶段的中间结构（inbox 引用或 pull 结果统一形态）。
type timelineEntry struct {
	postID    string
	createdAt time.Time
}

// getFeed 读取 timeline（§6 读路径 9 步）：解析 cursor → inbox 分页 → big_v(含自己) pull →
// merge 去重排序 → tombstone 过滤 → hydrate（内容/作者/计数）→ 返回 + 新 cursor。
func (s *Server) getFeed(c *gin.Context) {
	uid := UidFrom(c)
	limit := 20
	if v, err := strconv.Atoi(c.DefaultQuery("limit", "20")); err == nil && v > 0 && v <= 50 {
		limit = v
	}
	cur, err := decodeCursor(c.Query("cursor"))
	if err != nil {
		abort.AbortBadRequest(c)
		return
	}
	before := time.Now().Add(time.Second)
	if cur != nil {
		before = cur.ts
	}

	// 1) inbox push 部分
	inbox, err := s.feeds.PageInbox(c, uid, before, limit)
	if err != nil {
		log.Errorf("page inbox for %s: %v", uid, err)
		abort.AbortDatabaseError(c)
		return
	}
	entries := make([]timelineEntry, 0, len(inbox))
	for _, e := range inbox {
		entries = append(entries, timelineEntry{postID: e.PostID, createdAt: e.CreatedAt})
	}

	// 2) pull 部分：big_v 好友 + 自己（自己的帖子不经 inbox，约定见 T10 spec）
	friends, err := s.friends.ListByOwner(c, uid)
	if err != nil {
		log.Errorf("list friends for %s: %v", uid, err)
		abort.AbortDatabaseError(c)
		return
	}
	pullSet := []string{uid}
	for _, f := range friends {
		pullSet = append(pullSet, f.FriendUID)
	}
	pulled, err := s.feeds.PullRecentByAuthors(c, pullSet, limit)
	if err != nil {
		log.Errorf("pull recent for %s: %v", uid, err)
		abort.AbortDatabaseError(c)
		return
	}
	pullIDs := make([]string, 0, len(pulled))
	for _, p := range pulled {
		if p.CreatedAt.Before(before) || (p.CreatedAt.Equal(before) && p.PostID != cur.postID) {
			entries = append(entries, timelineEntry{postID: p.PostID, createdAt: p.CreatedAt})
			pullIDs = append(pullIDs, p.PostID)
		}
	}

	// 3) merge：按时间倒序去重（同 postID 保留一次）
	seen := make(map[string]bool, len(entries))
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].createdAt.Equal(entries[j].createdAt) {
			return entries[i].postID > entries[j].postID
		}
		return entries[i].createdAt.After(entries[j].createdAt)
	})
	merged := make([]timelineEntry, 0, len(entries))
	for _, e := range entries {
		if seen[e.postID] {
			continue
		}
		seen[e.postID] = true
		merged = append(merged, e)
		if len(merged) >= limit {
			break
		}
	}

	// 4) hydrate：批量内容 + 作者 + 计数；tombstone/缺失过滤
	postIDs := make([]string, 0, len(merged))
	for _, e := range merged {
		postIDs = append(postIDs, e.postID)
	}
	posts, err := s.feeds.GetPosts(c, postIDs)
	if err != nil {
		abort.AbortDatabaseError(c)
		return
	}
	counters, err := s.feeds.GetCounters(c, postIDs)
	if err != nil {
		abort.AbortDatabaseError(c)
		return
	}
	authorIDs := make([]string, 0)
	authorSet := map[string]bool{}
	items := make([]FeedItem, 0, len(merged))
	for _, e := range merged {
		p, ok := posts[e.postID]
		if !ok || p.IsDeleted {
			continue // tombstone / 缺失：读取时过滤（inbox 不物理清理）
		}
		if !authorSet[p.UID] {
			authorSet[p.UID] = true
			authorIDs = append(authorIDs, p.UID)
		}
		item := FeedItem{
			PostID:    p.PostID,
			UID:       p.UID,
			Content:   json.RawMessage(p.Content),
			Media:     json.RawMessage(p.Media),
			CreatedAt: p.CreatedAt,
		}
		if cnt := counters[p.PostID]; cnt != nil {
			item.LikeCnt, item.CommentCnt = cnt.LikeCnt, cnt.CommentCnt
		}
		items = append(items, item)
	}
	authors, err := s.users.GetMany(c, authorIDs)
	if err != nil {
		abort.AbortDatabaseError(c)
		return
	}
	for i := range items {
		if a := authors[items[i].UID]; a != nil {
			items[i].AuthorName = a.UserName
		}
	}

	// 5) 新 cursor：最后一项位置
	next := ""
	if len(items) == limit {
		last := items[len(items)-1]
		next = encodeCursor(last.CreatedAt, last.PostID)
	}
	c.JSON(http.StatusOK, gin.H{"posts": items, "next_cursor": next})
}
