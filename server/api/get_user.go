package api

import (
	"net/http"

	"alfred.brave.com/internal/abort"
	"github.com/gin-gonic/gin"
)

func (s *Server) GetUser(router *gin.RouterGroup) {
	router.GET("/user/:id", func(c *gin.Context) {
		uid := c.Param("id")
		user, err := s.users.Get(c, uid)
		if err != nil {
			log.Errorf("user: %s (get): %v", uid, err)
			abort.AbortDatabaseError(c)
			return
		}
		resp := &UserResponse{
			UID:       user.UID,
			CreatedAt: user.CreatedAt,
			UpdatedAt: user.UpdatedAt,
			LoginAt:   user.LoginAt,
			UserName:  user.UserName,
			Email:     user.Email,
			Profile:   user.Profile,
			Avatar:    user.Avatar,
			Friends:   make([]Friend, 0),
		}
		if err := s.AddFriends(c, resp); err != nil {
			abort.AbortUnexpected(c)
			return
		}
		c.JSON(http.StatusOK, resp)
	})
}

// AddFriends 填充用户响应的好友列表。
// 原版 Friend.UID 误用作 friend_uid（恒返回自身 uid），此处修正为 FriendUID 字段。
func (s *Server) AddFriends(c *gin.Context, resp *UserResponse) error {
	friends, err := s.friends.ListByOwner(c, resp.UID)
	if err != nil {
		log.Errorf("friends (list by %s): %v", resp.UID, err)
		return err
	}
	result := make([]Friend, 0)
	for _, f := range friends {
		info, err := s.users.Get(c, f.FriendUID)
		if err != nil {
			log.Errorf("friends: %s (get_user_info): %v", f.FriendUID, err)
			return err
		}
		result = append(result, Friend{
			FriendUID:     f.FriendUID,
			FriendName:    info.UserName,
			FriendProfile: info.Profile,
			FriendAvatar:  info.Avatar,
		})
	}
	resp.Friends = result
	return nil
}
