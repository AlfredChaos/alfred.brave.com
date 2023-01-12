package api

import (
	"net/http"

	"alfred.brave.com/internal/abort"
	"alfred.brave.com/internal/etcd"
	"github.com/gin-gonic/gin"
)

type UserLogin struct {
	UserId    string `json:"user_id"`
	UserToken string `json:"user_token"`
	LoginTime string `json:"login_time"`
}

func Login(router *gin.RouterGroup) {

	router.POST("/login", func(c *gin.Context) {
		var ul UserLogin
		if err := c.BindJSON(&ul); err != nil {
			abort.AbortBadRequest(c)
		}

		userFactory := etcd.UserFactory{
			Namespace: etcd.PrefixUsers,
			User: &etcd.User{
				UserId:         ul.UserId,
				UserToken:      ul.UserToken,
				LoginTime:      ul.LoginTime,
				LoginHost:      ServiceHost,
				JokerServiceId: ServiceId,
			},
		}
		if err := userFactory.Update(); err != nil {
			log.Errorf("user %s login fail", ul.UserId)
			abort.AbortLoginError(c)
			return
		}
		c.JSON(http.StatusOK, nil)
	})
}
