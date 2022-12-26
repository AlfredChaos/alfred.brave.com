package api

import (
	"net/http"
	"time"

	"alfred.brave.com/common"
	"alfred.brave.com/database"
	"github.com/gin-gonic/gin"
	"golang.org/x/crypto/bcrypt"
)

type UserRegister struct {
	UserName string `json:"user_name"`
	Email    string `json:"email"`
	Password string `json:"password"`
	Profile  string `json:"profile"`
}

type UserResponse struct {
	UID       string    `json:"uid"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
	LoginAt   time.Time `json:"login_at"`
	UserName  string    `json:"user_name"`
	Email     string    `json:"email"`
	Profile   string    `json:"profile"`
	Avatar    []byte    `json:"avatar"`
	Friends   []Friend  `json:"friends"`
}

type Friend struct {
	FriendUID     string `json:"friend_uid"`
	FriendName    string `json:"friend_name"`
	FriendProfile string `json:"friend_profile"`
	FriendAvatar  []byte `json:"friend_avatar"`
}

func Register(router *gin.RouterGroup) {
	router.POST("/register", func(c *gin.Context) {
		var ur UserRegister
		if err := c.BindJSON(&ur); err != nil {
			AbortBadRequest(c)
			return
		}

		if err := verifyRegisterParamter(ur); err != nil {
			AbortBadRequest(c)
			return
		}
		user := &database.User{}
		user.UserName = ur.UserName
		user.Email = ur.Email
		user.Profile = ur.Profile
		user.LoginAt = time.Now()
		user.LoginAt.Format(common.TimeFormat)
		hash, err := bcrypt.GenerateFromPassword([]byte(ur.Password), bcrypt.DefaultCost)
		if err != nil {
			log.Errorf("generate from password fail: %v", err)
			AbortBadRequest(c)
			return
		}
		user.Password = hash
		if err := user.Create(); err != nil {
			log.Errorf("user: %s (create): %v", err)
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
		c.JSON(http.StatusOK, resp)
	})
}

func verifyRegisterParamter(ur UserRegister) error {
	if err := verifyUserName(ur.UserName); err != nil {
		return err
	}
	if err := verifyEmail(ur.Email); err != nil {
		return err
	}
	if err := verifyPassword(ur.Password); err != nil {
		return err
	}
	return nil
}

func verifyUserName(name string) error {
	return nil
}

func verifyEmail(email string) error {
	return nil
}

func verifyPassword(password string) error {
	return nil
}
