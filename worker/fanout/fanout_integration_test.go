//go:build integration

// 集成测试：真实 PG 上的 fanout 闭环（好友 → inbox 批量写入 → big_v 跳过 → 重放幂等）。
package fanout

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"alfred.brave.com/database"
	"alfred.brave.com/feed/api"
)

func newFanoutEnv(t *testing.T) (*Worker, *database.Store) {
	t.Helper()
	dsn := os.Getenv("BRAVE_PG_DSN")
	if dsn == "" {
		dsn = "postgres://brave:brave@127.0.0.1:55432/brave?sslmode=disable"
	}
	store, err := database.NewStore(context.Background(), dsn, 4)
	if err != nil {
		t.Fatalf("connect pg: %v", err)
	}
	t.Cleanup(store.Close)
	return New(store, []string{"127.0.0.1:9092"}, fmt.Sprintf("fanout-w-it-%d", time.Now().UnixNano())), store
}

func fanoutUser(t *testing.T, store *database.Store) string {
	t.Helper()
	u := &database.User{UserName: fmt.Sprintf("fw%d", time.Now().UnixNano()), Email: fmt.Sprintf("%d@it.io", time.Now().UnixNano()), Password: []byte("x")}
	if err := database.NewUserStore(store).Create(context.Background(), u); err != nil {
		t.Fatalf("create user: %v", err)
	}
	return u.UID
}

// TestFanoutWritesInbox 好友三人 → 各自 inbox 出现该帖；发布者自己不进 inbox。
func TestFanoutWritesInbox(t *testing.T) {
	w, store := newFanoutEnv(t)
	ctx := context.Background()

	pub := fanoutUser(t, store)
	f1, f2, f3 := fanoutUser(t, store), fanoutUser(t, store), fanoutUser(t, store)
	fs := database.NewFriendStore(store)
	for _, f := range []string{f1, f2, f3} {
		if err := fs.Create(ctx, pub, f); err != nil {
			t.Fatalf("friend: %v", err)
		}
	}

	feeds := database.NewFeedStore(store)
	post, err := feeds.CreatePost(ctx, pub, []byte(`{"text":"fanout!"}`), []byte(`[]`), false)
	if err != nil {
		t.Fatalf("create post: %v", err)
	}

	if err := w.Handle(ctx, &api.FanoutEvent{PostID: post.PostID, PubUID: pub, CreatedAt: post.CreatedAt.Unix()}); err != nil {
		t.Fatalf("handle: %v", err)
	}

	for _, uid := range []string{f1, f2, f3} {
		entries, err := feeds.PageInbox(ctx, uid, time.Now().Add(time.Hour), 10)
		if err != nil || len(entries) != 1 || entries[0].PostID != post.PostID {
			t.Fatalf("inbox for friend: err=%v entries=%+v", err, entries)
		}
	}
	// 发布者自身不在收件箱（自己的帖子读自己的 timeline 不经 inbox——本实现约定）
	own, _ := feeds.PageInbox(ctx, pub, time.Now().Add(time.Hour), 10)
	if len(own) != 0 {
		t.Fatalf("publisher should not be in own inbox, got %d", len(own))
	}

	// 重放幂等：再跑一次不产生重复行
	if err := w.Handle(ctx, &api.FanoutEvent{PostID: post.PostID, PubUID: pub, CreatedAt: post.CreatedAt.Unix()}); err != nil {
		t.Fatalf("replay: %v", err)
	}
	entries, _ := feeds.PageInbox(ctx, f1, time.Now().Add(time.Hour), 10)
	if len(entries) != 1 {
		t.Fatalf("replay duplicated inbox rows: %d", len(entries))
	}
}

// TestFanoutSkipsBigV big_v 帖不写收件箱。
func TestFanoutSkipsBigV(t *testing.T) {
	w, store := newFanoutEnv(t)
	ctx := context.Background()

	pub := fanoutUser(t, store)
	friend := fanoutUser(t, store)
	if err := database.NewFriendStore(store).Create(ctx, pub, friend); err != nil {
		t.Fatalf("friend: %v", err)
	}

	feeds := database.NewFeedStore(store)
	post, err := feeds.CreatePost(ctx, pub, []byte(`{"text":"big"}`), []byte(`[]`), true)
	if err != nil {
		t.Fatalf("create post: %v", err)
	}
	if err := w.Handle(ctx, &api.FanoutEvent{PostID: post.PostID, PubUID: pub, CreatedAt: post.CreatedAt.Unix()}); err != nil {
		t.Fatalf("handle: %v", err)
	}
	entries, _ := feeds.PageInbox(ctx, friend, time.Now().Add(time.Hour), 10)
	if len(entries) != 0 {
		t.Fatalf("big_v post must skip inbox, got %d rows", len(entries))
	}
}
