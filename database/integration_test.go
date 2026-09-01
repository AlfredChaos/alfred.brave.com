//go:build integration

// 集成测试：连本地 PG（docker run postgres:15），先跑 goose 迁移再验证 CRUD 原行为等价。
//
//	运行：BRAVE_PG_DSN='postgres://brave:brave@127.0.0.1:55432/brave?sslmode=disable' \
//	     go test -tags=integration ./database/ -v
package database

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	dsn := os.Getenv("BRAVE_PG_DSN")
	if dsn == "" {
		dsn = "postgres://brave:brave@127.0.0.1:55432/brave?sslmode=disable"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open sql: %v", err)
	}
	if err := goose.SetDialect("postgres"); err != nil {
		t.Fatalf("goose dialect: %v", err)
	}
	if err := goose.Up(db, "migration"); err != nil {
		t.Fatalf("goose up: %v", err)
	}
	db.Close()

	store, err := NewStore(ctx, dsn, 4)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(store.Close)
	return store
}

func newTestUser(t *testing.T, us *UserStore, name, email string) *User {
	t.Helper()
	u := &User{
		UserName: name,
		Email:    email,
		Password: []byte("hashed-secret"),
		Profile:  "hello",
		LoginAt:  time.Now(),
	}
	if err := us.Create(context.Background(), u); err != nil {
		t.Fatalf("create user %s: %v", name, err)
	}
	return u
}

// TestUserCRUDEquivalence 原行为等价：create → get（三途径）→ list 过滤 → 唯一冲突。
func TestUserCRUDEquivalence(t *testing.T) {
	store := newTestStore(t)
	us := NewUserStore(store)
	ctx := context.Background()
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())

	alice := newTestUser(t, us, "alice"+suffix, "alice"+suffix+"@t.io")

	got, err := us.Get(ctx, alice.UID)
	if err != nil || got.UID != alice.UID || got.UserName != alice.UserName {
		t.Fatalf("Get by uid failed: %v %+v", err, got)
	}
	got, err = us.GetByUserName(ctx, alice.UserName)
	if err != nil || got.UID != alice.UID {
		t.Fatalf("GetByUserName failed: %v", err)
	}
	got, err = us.GetByEmail(ctx, alice.Email)
	if err != nil || got.UID != alice.UID {
		t.Fatalf("GetByEmail failed: %v", err)
	}

	if _, err := us.Get(ctx, "no-such-uid"); err != ErrNotFound {
		t.Fatalf("want ErrNotFound, got %v", err)
	}

	// 用户名唯一约束
	dup := &User{UserName: alice.UserName, Email: "other" + suffix + "@t.io", Password: []byte("x")}
	if err := us.Create(ctx, dup); err == nil {
		t.Fatal("duplicate user_name must fail")
	}

	// List 模糊过滤
	newTestUser(t, us, "bob"+suffix, "bob"+suffix+"@t.io")
	name := "alice" + suffix
	list, err := us.List(ctx, &UserFilters{UserName: &name})
	if err != nil || len(list) != 1 || list[0].UserName != alice.UserName {
		t.Fatalf("List filter failed: err=%v len=%d", err, len(list))
	}
	all, err := us.List(ctx, nil)
	if err != nil || len(all) < 2 {
		t.Fatalf("List all failed: err=%v len=%d", err, len(all))
	}

	// UpdateLoginAt
	if err := us.UpdateLoginAt(ctx, alice.UID); err != nil {
		t.Fatalf("UpdateLoginAt: %v", err)
	}
}

// TestFriendDoubleRow 好友双边两行：创建对称、删除双边、点查判定。
func TestFriendDoubleRow(t *testing.T) {
	store := newTestStore(t)
	us := NewUserStore(store)
	fs := NewFriendStore(store)
	ctx := context.Background()
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())

	alice := newTestUser(t, us, "fa"+suffix, "fa"+suffix+"@t.io")
	bob := newTestUser(t, us, "fb"+suffix, "fb"+suffix+"@t.io")
	carol := newTestUser(t, us, "fc"+suffix, "fc"+suffix+"@t.io")

	if err := fs.Create(ctx, alice.UID, bob.UID); err != nil {
		t.Fatalf("friend create: %v", err)
	}
	if _, err := fs.GetFriendship(ctx, alice.UID, bob.UID); err != nil {
		t.Fatalf("friendship A->B should exist: %v", err)
	}
	if _, err := fs.GetFriendship(ctx, bob.UID, alice.UID); err != nil {
		t.Fatalf("friendship B->A should exist (double row): %v", err)
	}
	friends, err := fs.ListByOwner(ctx, alice.UID)
	if err != nil || len(friends) != 1 || friends[0].FriendUID != bob.UID {
		t.Fatalf("ListByOwner failed: err=%v len=%d", err, len(friends))
	}

	// 幂等重建
	if err := fs.Create(ctx, alice.UID, bob.UID); err != nil {
		t.Fatalf("friend create idempotent: %v", err)
	}
	// 自加好友拒绝
	if err := fs.Create(ctx, alice.UID, alice.UID); err == nil {
		t.Fatal("self-friendship must be rejected")
	}

	if err := fs.Delete(ctx, alice.UID, bob.UID); err != nil {
		t.Fatalf("friend delete: %v", err)
	}
	if _, err := fs.GetFriendship(ctx, bob.UID, alice.UID); err != ErrNotFound {
		t.Fatalf("reverse row must be deleted too, got %v", err)
	}

	if err := fs.Create(ctx, carol.UID, alice.UID); err != nil {
		t.Fatalf("friend create carol: %v", err)
	}
}

// TestKvStore kv 基础语义：Put/Get/Delete/DeleteIfMatch/ScanPrefix。
func TestKvStore(t *testing.T) {
	store := newTestStore(t)
	kv := NewKvStore(store)
	ctx := context.Background()
	prefix := fmt.Sprintf("t:%d:", time.Now().UnixNano())

	type online struct {
		Cs   string `json:"cs"`
		Addr string `json:"addr"`
	}

	// Put + Get 往返
	if err := kv.Put(ctx, prefix+"u1", online{Cs: "cs-1", Addr: "10.0.0.1:37012"}); err != nil {
		t.Fatalf("kv put: %v", err)
	}
	var got online
	if err := kv.Get(ctx, prefix+"u1", &got); err != nil || got.Cs != "cs-1" {
		t.Fatalf("kv get: %v %+v", err, got)
	}
	if err := kv.Get(ctx, prefix+"none", &got); err != ErrNotFound {
		t.Fatalf("want ErrNotFound, got %v", err)
	}

	// 覆盖更新
	if err := kv.Put(ctx, prefix+"u1", online{Cs: "cs-2", Addr: "10.0.0.2:37012"}); err != nil {
		t.Fatalf("kv put overwrite: %v", err)
	}
	if err := kv.Get(ctx, prefix+"u1", &got); err != nil || got.Cs != "cs-2" {
		t.Fatalf("kv overwrite failed: %+v", got)
	}

	// 条件删除：cs 不匹配不删（漂移保护），匹配则删
	if err := kv.DeleteIfMatch(ctx, prefix+"u1", "cs", "cs-1"); err != nil {
		t.Fatalf("kv deleteIfMatch: %v", err)
	}
	if err := kv.Get(ctx, prefix+"u1", &got); err != nil {
		t.Fatalf("mismatched delete must keep record: %v", err)
	}
	if err := kv.DeleteIfMatch(ctx, prefix+"u1", "cs", "cs-2"); err != nil {
		t.Fatalf("kv deleteIfMatch: %v", err)
	}
	if err := kv.Get(ctx, prefix+"u1", &got); err != ErrNotFound {
		t.Fatalf("matched delete must remove record, got %v", err)
	}

	// 前缀扫描
	for i := 0; i < 3; i++ {
		if err := kv.Put(ctx, fmt.Sprintf("%ss%d", prefix, i), online{Cs: fmt.Sprintf("cs-%d", i)}); err != nil {
			t.Fatalf("kv put scan: %v", err)
		}
	}
	entries, err := kv.ScanPrefix(ctx, prefix, 100)
	if err != nil || len(entries) != 3 {
		t.Fatalf("ScanPrefix want 3, err=%v len=%d", err, len(entries))
	}
}

// TestKvNextSeq seq 原子自增：串行严格递增，并发不重号。
func TestKvNextSeq(t *testing.T) {
	store := newTestStore(t)
	kv := NewKvStore(store)
	ctx := context.Background()
	convID := fmt.Sprintf("conv-%d", time.Now().UnixNano())

	// 串行：首条为 1，之后 2、3
	for want := int64(1); want <= 3; want++ {
		got, err := kv.NextSeq(ctx, convID)
		if err != nil || got != want {
			t.Fatalf("NextSeq serial: got %d want %d (err=%v)", got, want, err)
		}
	}

	// 并发：50 goroutine 各取一次，结果必须两两不同
	const n = 50
	results := make(chan int64, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			seq, err := kv.NextSeq(ctx, convID)
			if err != nil {
				t.Errorf("NextSeq concurrent: %v", err)
				return
			}
			results <- seq
		}()
	}
	wg.Wait()
	close(results)
	seen := make(map[int64]bool)
	for seq := range results {
		if seen[seq] {
			t.Fatalf("duplicate seq allocated: %d", seq)
		}
		seen[seq] = true
	}
}

// TestConversationSingleKeyUnique 单聊会话唯一性：second insert 触发 ErrDuplicate。
func TestConversationSingleKeyUnique(t *testing.T) {
	store := newTestStore(t)
	cs := NewConversationStore(store)
	ctx := context.Background()
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	a, b := "uid-x"+suffix, "uid-y"+suffix

	if _, err := cs.CreateSingle(ctx, a, b); err != nil {
		t.Fatalf("create single conv: %v", err)
	}
	if _, err := cs.CreateSingle(ctx, b, a); err != ErrDuplicate {
		t.Fatalf("reversed pair must conflict, got %v", err)
	}
	got, err := cs.GetBySingleKey(ctx, a, b)
	if err != nil || got.Type != "single" {
		t.Fatalf("GetBySingleKey: %v %+v", err, got)
	}
}
