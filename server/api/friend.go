package api

import (
	"errors"
	"net/http"

	"alfred.brave.com/database"
	"alfred.brave.com/internal/abort"
	"github.com/gin-gonic/gin"
)

type FriendAdd struct {
	// UID 或 UserName 二选一，按名加好友供 Web 客户端使用
	UID      string `json:"uid"`
	UserName string `json:"user_name"`
}

// Friends 好友管理 API（鉴权保护）：加好友 / 好友列表 / 删好友。
// 数据面（fanout、会话）不经此 API，这里只维护关系表。
func (s *Server) Friends(router *gin.RouterGroup) {
	auth := router.Group("", s.AuthRequired())

	auth.POST("/friends", func(c *gin.Context) {
		uid := UidFrom(c)
		var body FriendAdd
		if err := c.BindJSON(&body); err != nil {
			abort.AbortBadRequest(c)
			return
		}
		target, err := s.resolveFriendTarget(c, body)
		if err != nil {
			abort.AbortBadRequest(c)
			return
		}
		if target.UID == uid {
			abort.AbortBadRequest(c)
			return
		}
		if err := s.friends.Create(c, uid, target.UID); err != nil {
			log.Errorf("friend create %s->%s: %v", uid, target.UID, err)
			abort.AbortDatabaseError(c)
			return
		}
		c.JSON(http.StatusOK, gin.H{"uid": target.UID, "user_name": target.UserName})
	})

	auth.GET("/friends", func(c *gin.Context) {
		resp := &UserResponse{UID: UidFrom(c), Friends: make([]Friend, 0)}
		if err := s.AddFriends(c, resp); err != nil {
			abort.AbortUnexpected(c)
			return
		}
		c.JSON(http.StatusOK, gin.H{"friends": resp.Friends})
	})

	auth.DELETE("/friends/:uid", func(c *gin.Context) {
		uid := UidFrom(c)
		target := c.Param("uid")
		if err := s.friends.Delete(c, uid, target); err != nil {
			log.Errorf("friend delete %s->%s: %v", uid, target, err)
			abort.AbortDatabaseError(c)
			return
		}
		c.JSON(http.StatusOK, gin.H{"deleted": target})
	})
}

// resolveFriendTarget 解析加好友目标：优先 uid，其次 user_name。
func (s *Server) resolveFriendTarget(c *gin.Context, body FriendAdd) (*database.User, error) {
	if body.UID != "" {
		return s.users.Get(c, body.UID)
	}
	if body.UserName != "" {
		return s.users.GetByUserName(c, body.UserName)
	}
	return nil, errors.New("uid or user_name required")
}

// Users 用户列表 API（鉴权保护）：Web 客户端选人用；限制返回条数。
func (s *Server) Users(router *gin.RouterGroup) {
	router.GET("/users", s.AuthRequired(), func(c *gin.Context) {
		users, err := s.users.List(c, nil)
		if err != nil {
			log.Errorf("users list: %v", err)
			abort.AbortDatabaseError(c)
			return
		}
		result := make([]gin.H, 0, len(users))
		for _, u := range users {
			result = append(result, gin.H{"uid": u.UID, "user_name": u.UserName, "email": u.Email})
		}
		c.JSON(http.StatusOK, gin.H{"users": result})
	})
}
