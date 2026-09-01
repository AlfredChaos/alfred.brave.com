// Package token 无状态 HMAC 令牌（网关鉴权用）。
//
// 设计取舍：不引入 JWT 库——需求只有 uid + 过期时间两个字段，
// 自签 HMAC-SHA256 三段式（uid.exp.sig）足够且零依赖；网关与 feed-api 共享同一 secret。
// 代价：无法主动吊销（练手项目可接受；要吊销需引入服务端会话表）。
package token

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"time"
)

var (
	ErrInvalid = errors.New("token: invalid")
	ErrExpired = errors.New("token: expired")
)

// Tokenizer HMAC 签发/校验器。secret 构造注入；实例并发安全（纯函数式调用）。
type Tokenizer struct {
	secret []byte
}

func New(secret string) *Tokenizer {
	return &Tokenizer{secret: []byte(secret)}
}

// Sign 签发 token：base64url(uid).base64url(expUnix).base64url(hmac)。
func (t *Tokenizer) Sign(uid string, ttl time.Duration) (string, error) {
	if uid == "" {
		return "", fmt.Errorf("token: uid required")
	}
	exp := time.Now().Add(ttl).Unix()
	payload := encode(uid) + "." + encode(strconv.FormatInt(exp, 10))
	return payload + "." + encode(t.sign(payload)), nil
}

// Parse 校验签名与有效期，返回 uid。
func (t *Tokenizer) Parse(signed string) (string, error) {
	p1 := indexOf(signed, '.')
	if p1 < 0 {
		return "", ErrInvalid
	}
	p2 := indexOf(signed[p1+1:], '.')
	if p2 < 0 {
		return "", ErrInvalid
	}
	uidB64, expB64, sigB64 := signed[:p1], signed[p1+1:p1+1+p2], signed[p1+1+p2+1:]
	if uidB64 == "" || expB64 == "" || sigB64 == "" {
		return "", ErrInvalid
	}

	payload := uidB64 + "." + expB64
	want := t.sign(payload)
	got, err := decode(sigB64)
	if err != nil || !hmac.Equal([]byte(got), []byte(want)) {
		return "", ErrInvalid
	}

	uid, err := decode(uidB64)
	if err != nil {
		return "", ErrInvalid
	}
	expB, err := decode(expB64)
	if err != nil {
		return "", ErrInvalid
	}
	exp, err := strconv.ParseInt(expB, 10, 64)
	if err != nil {
		return "", ErrInvalid
	}
	if time.Now().Unix() >= exp {
		return "", ErrExpired
	}
	return uid, nil
}

func (t *Tokenizer) sign(payload string) string {
	mac := hmac.New(sha256.New, t.secret)
	mac.Write([]byte(payload))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func encode(s string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(s))
}

func decode(s string) (string, error) {
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

func indexOf(s string, c byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == c {
			return i
		}
	}
	return -1
}
