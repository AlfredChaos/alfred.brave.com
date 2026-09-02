package api

import (
	"encoding/json"
	"net/http"

	"alfred.brave.com/database"
	"alfred.brave.com/internal/abort"

	"github.com/gin-gonic/gin"
)

type CommentCreate struct {
	Text string `json:"text"`
}

// registerActionRoutes 点赞/评论（T12）。
func (s *Server) registerActionRoutes(auth *gin.RouterGroup) {
	auth.POST("/feed/:id/like", s.toggleLike)
	auth.POST("/feed/:id/comments", s.addComment)
	auth.GET("/feed/:id/comments", s.listComments)
}

// toggleLike 点赞开关（幂等切换）。计数异步聚合（fanout worker 定时 flush），响应不回计数。
func (s *Server) toggleLike(c *gin.Context) {
	uid := UidFrom(c)
	postID := c.Param("id")

	// 帖子必须存在且未删除（对已删帖点赞无意义，404）
	posts, err := s.feeds.GetPosts(c, []string{postID})
	if err != nil {
		abort.AbortDatabaseError(c)
		return
	}
	if p := posts[postID]; p == nil || p.IsDeleted {
		abort.AbortNotFound(c)
		return
	}

	liked, err := s.feeds.ToggleLike(c, postID, uid)
	if err != nil {
		log.Errorf("toggle like post=%s user=%s: %v", postID, uid, err)
		abort.AbortDatabaseError(c)
		return
	}
	c.JSON(http.StatusOK, gin.H{"liked": liked})
}

// addComment 发评论（一人一帖一条，重复覆盖——最简版，spec 已声明取舍）。
func (s *Server) addComment(c *gin.Context) {
	uid := UidFrom(c)
	postID := c.Param("id")
	var body CommentCreate
	if err := c.BindJSON(&body); err != nil || len(body.Text) == 0 || len(body.Text) > 500 {
		abort.AbortBadRequest(c)
		return
	}

	raw, _ := json.Marshal(map[string]string{"text": body.Text})
	if err := s.feeds.AddComment(c, postID, uid, raw); err != nil {
		log.Errorf("add comment post=%s user=%s: %v", postID, uid, err)
		abort.AbortDatabaseError(c)
		return
	}
	c.JSON(http.StatusOK, gin.H{"commented": true})
}

// listComments 评论列表（正序 50 条，附作者名 hydrate）。
func (s *Server) listComments(c *gin.Context) {
	postID := c.Param("id")
	comments, err := s.feeds.ListComments(c, postID)
	if err != nil {
		log.Errorf("list comments post=%s: %v", postID, err)
		abort.AbortDatabaseError(c)
		return
	}
	uids := make([]string, 0, len(comments))
	seen := map[string]bool{}
	for _, cm := range comments {
		if !seen[cm.UID] {
			seen[cm.UID] = true
			uids = append(uids, cm.UID)
		}
	}
	authors, err := s.users.GetMany(c, uids)
	if err != nil {
		abort.AbortDatabaseError(c)
		return
	}
	result := make([]gin.H, 0, len(comments))
	for _, cm := range comments {
		name := ""
		if a := authors[cm.UID]; a != nil {
			name = a.UserName
		}
		result = append(result, gin.H{
			"uid": cm.UID, "user_name": name,
			"content": json.RawMessage(cm.Content), "created_at": cm.CreatedAt,
		})
	}
	c.JSON(http.StatusOK, gin.H{"comments": result})
}

var _ = database.ActionLike // 语义常量引用（ToggleLike 内联 SQL 使用同值）
