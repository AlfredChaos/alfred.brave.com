package api

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"alfred.brave.com/internal/abort"

	"github.com/gin-gonic/gin"
)

// bigVThreshold 大 V 阈值：好友数超过即写扩散降级读时 pull（§6 一条规则，不搞复杂策略）。
const bigVThreshold = 5000

type PostCreate struct {
	Text  string   `json:"text"`
	Media []string `json:"media"`
}

// FanoutEvent feed.fanout 载荷（fanout worker 消费）。
type FanoutEvent struct {
	PostID    string `json:"post_id"`
	PubUID    string `json:"pub_uid"`
	CreatedAt int64  `json:"created_at"`
}

// RegisterRoutes 挂载全部 feed API（发布 T09 / 读取 T11 / 动作 T12）。
func RegisterRoutes(router *gin.RouterGroup, s *Server) {
	auth := router.Group("", s.AuthRequired())
	auth.POST("/feed", s.createPost)
}

// createPost 发布（§6 步骤 3-6）：同步落库返回 post_id（不等 fanout），
// 异步 produce feed.fanout（key=pub_uid 保证同发布者串行）。
func (s *Server) createPost(c *gin.Context) {
	uid := UidFrom(c)
	var body PostCreate
	if err := c.BindJSON(&body); err != nil {
		abort.AbortBadRequest(c)
		return
	}
	if len(body.Text) == 0 || len(body.Text) > 2000 {
		abort.AbortBadRequest(c)
		return
	}

	// big_v 判定：好友数 > 5000 → 只标big_v，fanout 跳过 inbox 写入，读取时 pull
	friendCnt, err := s.feeds.CountFriendsByOwner(c, uid)
	if err != nil {
		log.Errorf("count friends for %s: %v", uid, err)
		abort.AbortDatabaseError(c)
		return
	}
	isBigV := friendCnt > bigVThreshold

	content, _ := json.Marshal(map[string]string{"text": body.Text})
	media, _ := json.Marshal(body.Media)
	post, err := s.feeds.CreatePost(c, uid, content, media, isBigV)
	if err != nil {
		log.Errorf("create post by %s: %v", uid, err)
		abort.AbortDatabaseError(c)
		return
	}

	// fanout 事件异步扩散；produce 失败不阻塞发布（fail-open：读侧 big_v pull 与下次发布兜底，
	// 损失的只是这一次 push，诚实记录在 spec）
	event := FanoutEvent{PostID: post.PostID, PubUID: uid, CreatedAt: post.CreatedAt.Unix()}
	if raw, err := json.Marshal(event); err == nil {
		ctx, cancel := context.WithTimeout(c, 5*time.Second)
		if err := s.fanout.Write(ctx, uid, raw); err != nil {
			log.Errorf("produce feed.fanout %s failed: %v", post.PostID, err)
		}
		cancel()
	}

	c.JSON(http.StatusOK, gin.H{
		"post_id":    post.PostID,
		"is_big_v":   post.IsBigV,
		"created_at": post.CreatedAt,
	})
}

// AuthRequired 与网关一致的 Bearer 鉴权（feed 与网关共享 AUTH_SECRET）。
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
			log.Warnf("feed auth rejected: %v", err)
			abort.AbortUnauthorized(c)
			return
		}
		c.Set("uid", uid)
		c.Next()
	}
}

// UidFrom 从上下文取鉴权后的 uid。
func UidFrom(c *gin.Context) string {
	v, _ := c.Get("uid")
	uid, _ := v.(string)
	return uid
}
