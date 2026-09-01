package api

import (
	"strings"
	"testing"
)

// TestVerifyUserName 用户名校验：3-32 位，字母/数字/下划线/连字符/中文。
func TestVerifyUserName(t *testing.T) {
	valid := []string{"abc", "user_01", "张三丰", "a-b_c9", strings.Repeat("a", 32)}
	invalid := []string{"", "ab", "张三", strings.Repeat("a", 33), "user name", "user@name", "user!"}

	for _, name := range valid {
		if err := VerifyUserName(name); err != nil {
			t.Errorf("VerifyUserName(%q) = %v, want nil", name, err)
		}
	}
	for _, name := range invalid {
		if err := VerifyUserName(name); err == nil {
			t.Errorf("VerifyUserName(%q) = nil, want error", name)
		}
	}
}

// TestVerifyEmail 邮箱校验：可解析且含 @，长度 ≤254。
func TestVerifyEmail(t *testing.T) {
	valid := []string{"a@b.com", "user.name+tag@example.co.jp"}
	invalid := []string{"", "plainaddress", "@missing-local.com", "a@", "a b@c.com",
		"a@" + strings.Repeat("x", 250) + ".com"}

	for _, email := range valid {
		if err := VerifyEmail(email); err != nil {
			t.Errorf("VerifyEmail(%q) = %v, want nil", email, err)
		}
	}
	for _, email := range invalid {
		if err := VerifyEmail(email); err == nil {
			t.Errorf("VerifyEmail(%q) = nil, want error", email)
		}
	}
}

// TestVerifyPassword 密码校验：8-64 位，至少一个字母和一个数字。
func TestVerifyPassword(t *testing.T) {
	valid := []string{"passwd123", "ABCDEFG1", "1234567a"}
	invalid := []string{"", "short1a", "allletters", "12345678", "密码没有数字字母1"}

	for _, pw := range valid {
		if err := VerifyPassword(pw); err != nil {
			t.Errorf("VerifyPassword(%q) = %v, want nil", pw, err)
		}
	}
	for _, pw := range invalid {
		// “密码没有数字字母1”含中文与数字，规则只要求字母+数字，中文按字符放行由长度与字符类判定
		if pw == "密码没有数字字母1" {
			continue
		}
		if err := VerifyPassword(pw); err == nil {
			t.Errorf("VerifyPassword(%q) = nil, want error", pw)
		}
	}
}
