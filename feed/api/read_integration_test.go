//go:build integration

// 集成测试：读取链路 merge/cursor/tombstone/hydrate。
package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"alfred.brave.com/database"

	"github.com/gin-gonic/gin"
)

// setupTimeline 构造数据：reader 与 pub1/pub2 好友（普通帖经 fanout 落 inbox）、
// bigv 作者（is_big_v 帖不落 inbox）、reader 自己的帖（pull 自集）。
func setupTimeline(t *testing.T, store *database.Store) (reader string, pub1Post, bigvPost, ownPost string) {
	var pub1, bigv string
	t.Helper()
	ctx := context.Background()
	feeds := database.NewFeedStore(store)
	friends := database.NewFriendStore(store)

	reader, _ = feedRegisterUser(t, store)
	pub1, _ = feedRegisterUser(t, store)
	bigv, _ = feedRegisterUser(t, store)

	for _, f := range []string{pub1, bigv} {
		if err := friends.Create(ctx, reader, f); err != nil {
			t.Fatalf("friend: %v", err)
		}
	}

	// 普通帖：fanout 已写 reader inbox（直接用 InsertInbox 模拟 fanout worker 行为）
	p1, err := feeds.CreatePost(ctx, pub1, []byte(`{"text":"from-pub1"}`), []byte(`[]`), false)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	if err := feeds.InsertInbox(ctx, reader, p1.PostID, p1.CreatedAt); err != nil {
		t.Fatalf("inbox: %v", err)
	}
	time.Sleep(2 * time.Millisecond) // 保证 created_at 严格递增（同毫秒兜底用 postID 排序）

	// big_v 帖：只落 posts
	p2, err := feeds.CreatePost(ctx, bigv, []byte(`{"text":"from-bigv"}`), []byte(`[]`), true)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	time.Sleep(2 * time.Millisecond)

	// 自己的帖
	p3, err := feeds.CreatePost(ctx, reader, []byte(`{"text":"own"}`), []byte(`[]`), false)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	return reader, p1.PostID, p2.PostID, p3.PostID
}

func feedGet(t *testing.T, router *gin.Engine, token, cursor string) (items []FeedItem, next string) {
	t.Helper()
	url := "/v1/feed"
	if cursor != "" {
		url += "?cursor=" + cursor
	}
	req := httptest.NewRequest(http.MethodGet, url, nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("GET /v1/feed -> %d: %s", w.Code, w.Body.String())
	}
	var resp struct {
		Posts      []FeedItem `json:"posts"`
		NextCursor string     `json:"next_cursor"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return resp.Posts, resp.NextCursor
}

// TestFeedReadMerge 第一页应同时包含 inbox 帖、big_v pull 帖、自己的帖，按时间倒序。
func TestFeedReadMerge(t *testing.T) {
	router, store := newFeedEnv(t)
	reader, pub1Post, bigvPost, ownPost := setupTimeline(t, store)
	_, token := tokenFor(t, store, reader)

	items, _ := feedGet(t, router, token, "")
	ids := map[string]int{}
	for i, it := range items {
		ids[it.PostID] = i
	}
	for _, want := range []string{pub1Post, bigvPost, ownPost} {
		if _, ok := ids[want]; !ok {
			t.Fatalf("post %s missing from timeline; items=%+v", want, ids)
		}
	}
	// 倒序：own(最新) 在 bigv 之前（index 更小）
	if ids[ownPost] > ids[bigvPost] || ids[bigvPost] > ids[pub1Post] {
		t.Fatalf("timeline not sorted desc: own=%d bigv=%d pub1=%d", ids[ownPost], ids[bigvPost], ids[pub1Post])
	}
}

// TestFeedReadCursorPagination limit=2 分页：两页无重叠且覆盖全部三帖。
func TestFeedReadCursorPagination(t *testing.T) {
	router, store := newFeedEnv(t)
	reader, pub1Post, bigvPost, ownPost := setupTimeline(t, store)
	_, token := tokenFor(t, store, reader)

	got := map[string]bool{}
	items1, next := feedGetLimit(t, router, token, "", 2)
	if len(items1) != 2 || next == "" {
		t.Fatalf("page1: len=%d next=%q", len(items1), next)
	}
	for _, it := range items1 {
		got[it.PostID] = true
	}
	items2, next2 := feedGetLimit(t, router, token, next, 2)
	for _, it := range items2 {
		if got[it.PostID] {
			t.Fatal("cursor page overlap")
		}
		got[it.PostID] = true
	}
	if next2 != "" {
		t.Fatalf("expected exhausted pagination, got next=%q", next2)
	}
	for _, want := range []string{pub1Post, bigvPost, ownPost} {
		if !got[want] {
			t.Fatalf("post %s not covered by pagination", want)
		}
	}
}

// TestFeedReadTombstoneFilter 删除的帖从 timeline 消失（inbox 不物理清理）。
func TestFeedReadTombstoneFilter(t *testing.T) {
	router, store := newFeedEnv(t)
	reader, pub1Post, _, _ := setupTimeline(t, store)
	_, token := tokenFor(t, store, reader)

	if err := database.NewFeedStore(store).SoftDeletePost(context.Background(), pub1Post); err != nil {
		t.Fatalf("soft delete: %v", err)
	}
	items, _ := feedGet(t, router, token, "")
	for _, it := range items {
		if it.PostID == pub1Post {
			t.Fatal("tombstoned post must be filtered from timeline")
		}
	}
}

// feedGetLimit 带自定义 limit 的读取。
func feedGetLimit(t *testing.T, router *gin.Engine, token, cursor string, limit int) ([]FeedItem, string) {
	t.Helper()
	url := fmt.Sprintf("/v1/feed?limit=%d", limit)
	if cursor != "" {
		url += "&cursor=" + cursor
	}
	req := httptest.NewRequest(http.MethodGet, url, nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("GET -> %d", w.Code)
	}
	var resp struct {
		Posts      []FeedItem `json:"posts"`
		NextCursor string     `json:"next_cursor"`
	}
	json.Unmarshal(w.Body.Bytes(), &resp)
	return resp.Posts, resp.NextCursor
}

// tokenFor 按已知 uid 取 token。
func tokenFor(t *testing.T, store *database.Store, uid string) (string, string) {
	t.Helper()
	return uid, signToken(uid)
}
