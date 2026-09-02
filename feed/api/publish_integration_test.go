//go:build integration

// 集成测试：真实 PG + Kafka 的发布链路（handler 级，经 httptest 直打 feed API）。
package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"alfred.brave.com/database"
	"alfred.brave.com/internal/chat"

	"github.com/gin-gonic/gin"
	"github.com/segmentio/kafka-go"
)

func newFeedEnv(t *testing.T) (*gin.Engine, *database.Store) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	dsn := os.Getenv("BRAVE_PG_DSN")
	if dsn == "" {
		dsn = "postgres://brave:brave@127.0.0.1:55432/brave?sslmode=disable"
	}
	store, err := database.NewStore(context.Background(), dsn, 4)
	if err != nil {
		t.Fatalf("connect pg: %v", err)
	}
	t.Cleanup(store.Close)

	brokers := []string{"127.0.0.1:9092"}
	if env := os.Getenv("BRAVE_KAFKA_BROKERS"); env != "" {
		brokers = []string{env}
	}

	itStore = store
	router := gin.New()
	v1 := router.Group("/v1")
	srv := NewServer(store, "it-secret", brokers)
	RegisterRoutes(v1, srv)
	return router, store
}

func feedRegisterUser(t *testing.T, store *database.Store) (uid, token string) {
	t.Helper()
	name := fmt.Sprintf("fp%d", time.Now().UnixNano())
	u := &database.User{UserName: name, Email: name + "@it.io", Password: []byte("x")}
	if err := database.NewUserStore(store).Create(context.Background(), u); err != nil {
		t.Fatalf("create user: %v", err)
	}
	return u.UID, signToken(u.UID)
}

// TestPublishPost 发布成功：posts 行存在、text 落 JSONB、fanout 事件进 topic（key=pub_uid）。
func TestPublishPost(t *testing.T) {
	router, store := newFeedEnv(t)
	uid, token := feedRegisterUser(t, store)

	body := fmt.Sprintf(`{"text":"hello feed","media":["img://1"]}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/feed", strReader(body))
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("publish -> %d: %s", w.Code, w.Body.String())
	}
	var resp struct {
		PostID string `json:"post_id"`
	}
	json.Unmarshal(w.Body.Bytes(), &resp)
	if resp.PostID == "" {
		t.Fatal("missing post_id")
	}

	posts, err := database.NewFeedStore(store).GetPosts(context.Background(), []string{resp.PostID})
	if err != nil || posts[resp.PostID] == nil {
		t.Fatalf("post not stored: %v", err)
	}
	var stored map[string]string
	if err := json.Unmarshal(posts[resp.PostID].Content, &stored); err != nil || stored["text"] != "hello feed" {
		t.Fatalf("content mismatch: %s", posts[resp.PostID].Content)
	}

	// fanout 事件进 Kafka（独立消费组验证一条）
	brokers := []string{"127.0.0.1:9092"}
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers, GroupID: fmt.Sprintf("fanout-it-%d", time.Now().UnixNano()), Topic: chat.TopicFanout,
	})
	defer reader.Close()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		m, err := reader.FetchMessage(ctx)
		cancel()
		if err != nil {
			continue
		}
		var ev struct {
			PostID string `json:"post_id"`
			PubUID string `json:"pub_uid"`
		}
		if json.Unmarshal(m.Value, &ev) != nil || ev.PostID != resp.PostID {
			_ = reader.CommitMessages(context.Background(), m)
			continue
		}
		if string(m.Key) != uid {
			t.Fatalf("fanout key = %s, want pub_uid %s", m.Key, uid)
		}
		return // PASS
	}
	t.Fatal("fanout event not seen in kafka")
}

// TestPublishAuth 未带 token 401；空文本 400。
func TestPublishAuth(t *testing.T) {
	router, _ := newFeedEnv(t)

	req := httptest.NewRequest(http.MethodPost, "/v1/feed", strReader(`{"text":"x"}`))
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("no token -> %d, want 401", w.Code)
	}

	_, token := feedRegisterUser(t, mustStore(t))
	req = httptest.NewRequest(http.MethodPost, "/v1/feed", strReader(`{"text":""}`))
	req.Header.Set("Authorization", "Bearer "+token)
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("empty text -> %d, want 400", w.Code)
	}
}
