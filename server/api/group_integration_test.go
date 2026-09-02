//go:build integration

// 集成测试：群管理权限矩阵（§5）+ 群事件消息写入（D09）。
package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"alfred.brave.com/database"
	"alfred.brave.com/internal/token"

	"github.com/gin-gonic/gin"
)

type groupEnv struct {
	router  *gin.Engine
	store   *database.Store
	owner   string
	member  string
	outside string
	gid     string
}

func newGroupEnv(t *testing.T) *groupEnv {
	t.Helper()
	gin.SetMode(gin.TestMode)
	store, err := database.NewStore(context.Background(), "postgres://brave:brave@127.0.0.1:55432/brave?sslmode=disable", 4)
	if err != nil {
		t.Fatalf("connect pg: %v", err)
	}
	t.Cleanup(store.Close)

	router := gin.New()
	v1 := router.Group("/v1")
	srv := NewServer(store, "gw-it-secret", []string{"127.0.0.1:9092"}, 7)
	srv.Register(v1)
	srv.Groups(v1)

	e := &groupEnv{router: router, store: store}
	e.owner = groupUser(t, store)
	e.member = groupUser(t, store)
	e.outside = groupUser(t, store)

	req := httptest.NewRequest(http.MethodPost, "/v1/groups", strReaderG(
		fmt.Sprintf(`{"name":"test-group","members":["%s"]}`, e.member)))
	req.Header.Set("Authorization", "Bearer "+gwSign(e.owner))
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("create group -> %d %s", w.Code, w.Body.String())
	}
	var resp struct {
		GID string `json:"gid"`
	}
	json.Unmarshal(w.Body.Bytes(), &resp)
	e.gid = resp.GID
	if e.gid == "" {
		t.Fatal("missing gid")
	}
	return e
}

func groupUser(t *testing.T, store *database.Store) string {
	t.Helper()
	name := fmt.Sprintf("gp%d", time.Now().UnixNano())
	u := &database.User{UserName: name, Email: name + "@it.io", Password: []byte("x")}
	if err := database.NewUserStore(store).Create(context.Background(), u); err != nil {
		t.Fatalf("create user: %v", err)
	}
	return u.UID
}

func gwSign(uid string) string {
	s, err := token.New("gw-it-secret").Sign(uid, time.Hour)
	if err != nil {
		panic(err)
	}
	return s
}

func strReaderG(s string) *strings.Reader { return strings.NewReader(s) }

func (e *groupEnv) call(t *testing.T, method, path, uid, body string) int {
	t.Helper()
	var req *http.Request
	if body == "" {
		req = httptest.NewRequest(method, path, nil)
	} else {
		req = httptest.NewRequest(method, path, strReaderG(body))
	}
	req.Header.Set("Authorization", "Bearer "+gwSign(uid))
	w := httptest.NewRecorder()
	e.router.ServeHTTP(w, req)
	return w.Code
}

// TestGroupPermissionMatrix §5 权限矩阵逐行验证。
func TestGroupPermissionMatrix(t *testing.T) {
	e := newGroupEnv(t)
	gid := e.gid

	cases := []struct {
		name string
		perm bool // true=应放行(200) false=应拒绝(403/409/400)
		do   func() int
	}{
		// 拉人：owner ✓ / member ✗ / 外人 ✗
		{"owner adds member", true, func() int {
			return e.call(t, http.MethodPost, "/v1/groups/"+gid+"/members", e.owner,
				fmt.Sprintf(`{"uids":["%s"]}`, e.outside))
		}},
		{"member cannot add", false, func() int {
			return e.call(t, http.MethodPost, "/v1/groups/"+gid+"/members", e.member, `{"uids":["x"]}`)
		}},
		// 踢人：owner ✓（踢 member）；不可踢 owner；member ✗
		{"owner kicks member", true, func() int {
			return e.call(t, http.MethodDelete, "/v1/groups/"+gid+"/members/"+e.member, e.owner, "")
		}},
		{"owner cannot kick self", false, func() int {
			return e.call(t, http.MethodDelete, "/v1/groups/"+gid+"/members/"+e.owner, e.owner, "")
		}},
		{"member cannot kick", false, func() int {
			return e.call(t, http.MethodDelete, "/v1/groups/"+gid+"/members/"+e.outside, e.member, "")
		}},
		// 改名/公告/置顶：owner ✓ / member ✗
		{"owner renames", true, func() int {
			return e.call(t, http.MethodPatch, "/v1/groups/"+gid, e.owner, `{"name":"new-name"}`)
		}},
		{"member cannot rename", false, func() int {
			return e.call(t, http.MethodPatch, "/v1/groups/"+gid, e.member, `{"name":"hack"}`)
		}},
		{"owner announces", true, func() int {
			return e.call(t, http.MethodPut, "/v1/groups/"+gid+"/announcement", e.owner, `{"text":"hi"}`)
		}},
		{"member cannot announce", false, func() int {
			return e.call(t, http.MethodPut, "/v1/groups/"+gid+"/announcement", e.member, `{"text":"x"}`)
		}},
		{"owner pins", true, func() int {
			return e.call(t, http.MethodPut, "/v1/groups/"+gid+"/pinned", e.owner, `{"msg_id":"m-1"}`)
		}},
		// 退群：member ✓ / owner ✗（只能解散）——e.member 已被踢，用后加入的 outside
		{"member quits", true, func() int {
			return e.call(t, http.MethodPost, "/v1/groups/"+gid+"/quit", e.outside, "")
		}},
		{"owner cannot quit", false, func() int {
			return e.call(t, http.MethodPost, "/v1/groups/"+gid+"/quit", e.owner, "")
		}},
		// 解散：owner ✓ / member ✗
		{"member cannot dismiss", false, func() int {
			return e.call(t, http.MethodDelete, "/v1/groups/"+gid, e.member, "")
		}},
		{"owner dismisses", true, func() int {
			return e.call(t, http.MethodDelete, "/v1/groups/"+gid, e.owner, "")
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.do()
			if tc.perm && got != http.StatusOK {
				t.Fatalf("expected 200, got %d", got)
			}
			if !tc.perm && got < 400 {
				t.Fatalf("expected rejection (4xx), got %d", got)
			}
		})
	}
}

// TestGroupEventMessagesAsSeqStream 管理操作产生 system_event 消息且共享 seq 流（D09）。
func TestGroupEventMessagesAsSeqStream(t *testing.T) {
	e := newGroupEnv(t)
	ctx := context.Background()

	msgs := database.NewMessageStore(e.store)
	list, err := msgs.ListAfter(ctx, e.gid, 0, 50)
	if err != nil || len(list) != 1 {
		t.Fatalf("create-group event expected: err=%v len=%d", err, len(list))
	}
	if list[0].Type != "system_event" || list[0].Seq != 1 {
		t.Fatalf("event msg wrong: %+v", list[0])
	}

	// 拉人 → 第二条事件 seq=2
	if code := e.call(t, http.MethodPost, "/v1/groups/"+e.gid+"/members", e.owner,
		fmt.Sprintf(`{"uids":["%s"]}`, e.outside)); code != 200 {
		t.Fatalf("add member -> %d", code)
	}
	list, _ = msgs.ListAfter(ctx, e.gid, 0, 50)
	if len(list) != 2 || list[1].Seq != 2 || list[1].Type != "system_event" {
		t.Fatalf("join event expected at seq=2: len=%d", len(list))
	}
}

// TestGroupDismissBlocksPersist 解散后状态收敛（persist 校验在 T14 联测，这里验证 tombstone）。
func TestGroupDismissBlocksPersist(t *testing.T) {
	e := newGroupEnv(t)
	if code := e.call(t, http.MethodDelete, "/v1/groups/"+e.gid, e.owner, ""); code != 200 {
		t.Fatalf("dismiss -> %d", code)
	}
	g, err := database.NewGroupStore(e.store).GetGroup(context.Background(), e.gid)
	if err != nil || g.Status != database.GroupStatusDismissed {
		t.Fatalf("group status: %v %+v", err, g)
	}
	// 解散事件是最后一条消息
	msgs, _ := database.NewMessageStore(e.store).ListAfter(context.Background(), e.gid, 0, 50)
	if len(msgs) != 2 || msgs[1].Seq != 2 {
		t.Fatalf("dismiss event expected: len=%d", len(msgs))
	}
	var content map[string]interface{}
	json.Unmarshal(msgs[1].Content, &content)
	if content["event"] != database.EventGroupDismiss {
		t.Fatalf("dismiss event content: %v", content)
	}
}
