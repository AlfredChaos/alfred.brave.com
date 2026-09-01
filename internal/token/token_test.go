package token

import (
	"strings"
	"testing"
	"time"
)

// TestSignParseRoundtrip 签发→解析往返，uid 一致。
func TestSignParseRoundtrip(t *testing.T) {
	tk := New("test-secret")
	signed, err := tk.Sign("user-1", time.Hour)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	uid, err := tk.Parse(signed)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if uid != "user-1" {
		t.Fatalf("uid = %q, want user-1", uid)
	}
}

// TestParseExpired 过期 token 必须被拒绝。
func TestParseExpired(t *testing.T) {
	tk := New("test-secret")
	signed, err := tk.Sign("user-1", -time.Minute)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	if _, err := tk.Parse(signed); err == nil {
		t.Fatal("expired token must be rejected")
	}
}

// TestParseTampered 篡改载荷/签名必须被拒绝。
func TestParseTampered(t *testing.T) {
	tk := New("test-secret")
	signed, _ := tk.Sign("user-1", time.Hour)

	parts := strings.Split(signed, ".")
	if len(parts) != 3 {
		t.Fatalf("token format: %q", signed)
	}
	// 篡改 uid 段
	tampered := "dXNlci0y." + parts[1] + "." + parts[2]
	if _, err := tk.Parse(tampered); err == nil {
		t.Fatal("tampered payload must be rejected")
	}
	// 篡改签名段
	tampered = parts[0] + "." + parts[1] + ".aW52YWxpZA"
	if _, err := tk.Parse(tampered); err == nil {
		t.Fatal("tampered signature must be rejected")
	}
}

// TestParseWrongSecret 不同密钥签发的 token 必须被拒绝。
func TestParseWrongSecret(t *testing.T) {
	signer := New("secret-a")
	verifier := New("secret-b")
	signed, _ := signer.Sign("user-1", time.Hour)
	if _, err := verifier.Parse(signed); err == nil {
		t.Fatal("token signed by different secret must be rejected")
	}
}

// TestParseGarbage 非 token 字符串不 panic、返回错误。
func TestParseGarbage(t *testing.T) {
	tk := New("test-secret")
	for _, bad := range []string{"", "abc", "a.b", "a.b.c.d", ".."} {
		if _, err := tk.Parse(bad); err == nil {
			t.Fatalf("garbage %q must be rejected", bad)
		}
	}
}
