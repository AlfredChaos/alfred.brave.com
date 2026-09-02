//go:build integration

// 集成测试：点赞开关 / 评论 / 计数聚合 flush。
package api

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"alfred.brave.com/database"
)

// TestLikeToggleAndCounters 点赞→计数聚合后 like_cnt=1；取消→flush 后=0。
func TestLikeToggleAndCounters(t *testing.T) {
	router, store := newFeedEnv(t)
	ctx := context.Background()

	author, _ := feedRegisterUser(t, store)
	_, likerToken := feedRegisterUser(t, store)

	feeds := database.NewFeedStore(store)
	post, err := feeds.CreatePost(ctx, author, []byte(`{"text":"like me"}`), []byte(`[]`), false)
	if err != nil {
		t.Fatalf("create post: %v", err)
	}

	// 点赞
	req := httptest.NewRequest(http.MethodPost, "/v1/feed/"+post.PostID+"/like", nil)
	req.Header.Set("Authorization", "Bearer "+likerToken)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != 200 || !jsonHas(w.Body.String(), `"liked":true`) {
		t.Fatalf("like -> %d %s", w.Code, w.Body.String())
	}

	// 计数聚合（直接调 flush 等价物）
	if err := feeds.RecountPostCounters(ctx, []string{post.PostID}); err != nil {
		t.Fatalf("recount: %v", err)
	}
	counters, _ := feeds.GetCounters(ctx, []string{post.PostID})
	if c := counters[post.PostID]; c == nil || c.LikeCnt != 1 {
		t.Fatalf("like_cnt want 1, got %+v", c)
	}

	// 取消点赞 → 再聚合 → 0
	req = httptest.NewRequest(http.MethodPost, "/v1/feed/"+post.PostID+"/like", nil)
	req.Header.Set("Authorization", "Bearer "+likerToken)
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != 200 || !jsonHas(w.Body.String(), `"liked":false`) {
		t.Fatalf("unlike -> %d %s", w.Code, w.Body.String())
	}
	if err := feeds.RecountPostCounters(ctx, []string{post.PostID}); err != nil {
		t.Fatalf("recount: %v", err)
	}
	counters, _ = feeds.GetCounters(ctx, []string{post.PostID})
	if c := counters[post.PostID]; c == nil || c.LikeCnt != 0 {
		t.Fatalf("like_cnt want 0 after unlike, got %+v", c)
	}
}

// TestCommentFlow 评论写入→列表→计数。
func TestCommentFlow(t *testing.T) {
	router, store := newFeedEnv(t)
	ctx := context.Background()

	author, _ := feedRegisterUser(t, store)
	_, commenterToken := feedRegisterUser(t, store)

	feeds := database.NewFeedStore(store)
	post, err := feeds.CreatePost(ctx, author, []byte(`{"text":"comment me"}`), []byte(`[]`), false)
	if err != nil {
		t.Fatalf("create post: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/v1/feed/%s/comments", post.PostID), strReader(`{"text":"nice!"}`))
	req.Header.Set("Authorization", "Bearer "+commenterToken)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("comment -> %d %s", w.Code, w.Body.String())
	}

	// 列表
	req = httptest.NewRequest(http.MethodGet, fmt.Sprintf("/v1/feed/%s/comments", post.PostID), nil)
	req.Header.Set("Authorization", "Bearer "+commenterToken)
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != 200 || !jsonHas(w.Body.String(), "nice!") {
		t.Fatalf("list comments -> %d %s", w.Code, w.Body.String())
	}

	// 计数
	if err := feeds.RecountPostCounters(ctx, []string{post.PostID}); err != nil {
		t.Fatalf("recount: %v", err)
	}
	counters, _ := feeds.GetCounters(ctx, []string{post.PostID})
	if c := counters[post.PostID]; c == nil || c.CommentCnt != 1 {
		t.Fatalf("comment_cnt want 1, got %+v", c)
	}
}

func jsonHas(s, sub string) bool {
	return len(s) >= len(sub) && (func() bool {
		for i := 0; i+len(sub) <= len(s); i++ {
			if s[i:i+len(sub)] == sub {
				return true
			}
		}
		return false
	})()
}
