package api

import (
	"net/http"

	"alfred.brave.com/database"
	"github.com/gin-gonic/gin"
)

func GetUser(router *gin.RouterGroup) {
	router.GET("/user/:id", func(c *gin.Context) {
		uid := c.Param("id")
		user := &database.User{}
		if err := user.Get(uid); err != nil {
			log.Errorf("user: %s (get): %v", uid, err)
			AbortUnexpected(c)
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
		if err := AddFriends(c, resp); err != nil {
			AbortUnexpected(c)
			return
		}
		c.JSON(http.StatusOK, resp)
	})
}

func AddFriends(c *gin.Context, resp *UserResponse) error {
	filters := database.FriendFilters{OwnerUID: &resp.UID}
	friend := &database.Friend{}
	friends := make([]database.Friend, 0)
	result := make([]Friend, 0)
	if err := friend.List(&filters, friends); err != nil {
		log.Errorf("friends (list): %v", err)
		return err
	}
	for _, f := range friends {
		info := &database.User{}
		if err := info.Get(f.FriendUID); err != nil {
			log.Errorf("friends: %s (get_user_info): %v", f.FriendUID, err)
			return err
		}
		res := Friend{
			FriendUID:     f.UID,
			FriendName:    info.UserName,
			FriendProfile: info.Profile,
			FriendAvatar:  info.Avatar,
		}
		result = append(result, res)
	}
	resp.Friends = result
	return nil
}
