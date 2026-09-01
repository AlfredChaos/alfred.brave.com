package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"alfred.brave.com/internal/token"
	"github.com/gin-gonic/gin"
)

func init() {
	gin.SetMode(gin.TestMode)
}

// newTestTokenizer 测试用固定密钥 tokenizer。
func newTestTokenizer() *token.Tokenizer {
	return token.New("test-secret")
}

// TestAuthMiddleware 鉴权中间件：无 token/坏 token 401，有效 token 放行并注入 uid。
func TestAuthMiddleware(t *testing.T) {
	srv := &Server{tokenizer: newTestTokenizer()}
	signed, _ := srv.tokenizer.Sign("user-1", time.Hour)

	cases := []struct {
		name   string
		header string
		want   int
	}{
		{"no header", "", http.StatusUnauthorized},
		{"not bearer", signed, http.StatusUnauthorized},
		{"invalid token", "Bearer garbage", http.StatusUnauthorized},
		{"valid token", "Bearer " + signed, http.StatusOK},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			router := gin.New()
			router.GET("/ping", srv.AuthRequired(), func(c *gin.Context) {
				uid, _ := c.Get("uid")
				c.String(http.StatusOK, "uid=%v", uid)
			})
			req := httptest.NewRequest(http.MethodGet, "/ping", nil)
			if tc.header != "" {
				req.Header.Set("Authorization", tc.header)
			}
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)
			if w.Code != tc.want {
				t.Fatalf("code = %d, want %d", w.Code, tc.want)
			}
			if tc.want == http.StatusOK {
				if got := w.Body.String(); got != "uid=user-1" {
					t.Fatalf("body = %q, want uid=user-1", got)
				}
			}
		})
	}
}

// TestAuthMiddlewareExpired 过期 token 401。
func TestAuthMiddlewareExpired(t *testing.T) {
	srv := &Server{tokenizer: newTestTokenizer()}
	signed, _ := srv.tokenizer.Sign("user-1", -time.Minute)

	router := gin.New()
	router.GET("/ping", srv.AuthRequired(), func(c *gin.Context) {
		c.String(http.StatusOK, "ok")
	})
	req := httptest.NewRequest(http.MethodGet, "/ping", nil)
	req.Header.Set("Authorization", "Bearer "+signed)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("code = %d, want 401", w.Code)
	}
}
