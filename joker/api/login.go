package api

import (
	"net/http"

	"alfred.brave.com/internal/abort"
	"alfred.brave.com/internal/etcd"
	"alfred.brave.com/internal/http_client"
	jkCommon "alfred.brave.com/joker/common"
	"github.com/gin-gonic/gin"
)

func Login(router *gin.RouterGroup) {

	router.POST("/login", func(c *gin.Context) {
		var ul http_client.UserLogin
		if err := c.BindJSON(&ul); err != nil {
			abort.AbortBadRequest(c)
		}

		userFactory := etcd.UserFactory{
			Namespace: etcd.PrefixUsers,
			User: &etcd.User{
				UserId:         ul.UserId,
				UserToken:      ul.UserToken,
				LoginTime:      ul.LoginTime,
				LoginHost:      jkCommon.ServiceHost,
				JokerServiceId: jkCommon.ServiceId,
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
