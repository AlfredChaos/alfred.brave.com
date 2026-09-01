package database

import "testing"

// TestSingleKey 单聊会话 key 规范化：与参数顺序无关（A→B 与 B→A 必须得到同一 key）。
func TestSingleKey(t *testing.T) {
	if SingleKey("alice", "bob") != SingleKey("bob", "alice") {
		t.Fatalf("SingleKey must be order-insensitive: %q vs %q",
			SingleKey("alice", "bob"), SingleKey("bob", "alice"))
	}
	if SingleKey("alice", "bob") != "alice:bob" {
		t.Fatalf("lexicographic order expected, got %q", SingleKey("alice", "bob"))
	}
}
