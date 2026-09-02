//go:build integration

package api

import (
	"strings"
	"testing"
	"time"

	"alfred.brave.com/database"
	"alfred.brave.com/internal/token"
)

// 共享的测试环境句柄（newFeedEnv 存的 store，供需要直连 PG 的用例复用）。
var itStore *database.Store

func newFeedTokenizer() *token.Tokenizer {
	return token.New("it-secret")
}

func strReader(s string) *strings.Reader {
	return strings.NewReader(s)
}

func mustStore(t *testing.T) *database.Store {
	t.Helper()
	if itStore == nil {
		t.Fatal("store not initialized; call newFeedEnv first")
	}
	return itStore
}

// signToken 测试辅助：签发 1 小时 token。
func signToken(uid string) string {
	s, err := newFeedTokenizer().Sign(uid, time.Hour)
	if err != nil {
		panic(err)
	}
	return s
}
