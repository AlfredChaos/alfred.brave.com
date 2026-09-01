package api

import (
	"alfred.brave.com/internal/abort"

	"github.com/gin-gonic/gin"
)

// AuthRequired Bearer token 鉴权中间件（T03 网关化）。
// 通过后把 uid 注入 gin.Context（key "uid"），业务 handler 用 UidFrom 取。
func (s *Server) AuthRequired() gin.HandlerFunc {
	return func(c *gin.Context) {
		raw := c.GetHeader("Authorization")
		const prefix = "Bearer "
		if len(raw) <= len(prefix) || raw[:len(prefix)] != prefix {
			abort.AbortUnauthorized(c)
			return
		}
		uid, err := s.tokenizer.Parse(raw[len(prefix):])
		if err != nil {
			log.Warnf("auth rejected: %v", err)
			abort.AbortUnauthorized(c)
			return
		}
		c.Set("uid", uid)
		c.Next()
	}
}

// UidFrom 从上下文取鉴权后的 uid；缺失说明路由未挂鉴权中间件（编码错误，显式失败）。
func UidFrom(c *gin.Context) string {
	v, ok := c.Get("uid")
	if !ok {
		return ""
	}
	uid, _ := v.(string)
	return uid
}
