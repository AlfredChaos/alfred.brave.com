package api

import (
	"errors"
	"net/http"
	"strings"

	"alfred.brave.com/database"
	"alfred.brave.com/internal/abort"
	"alfred.brave.com/internal/etcd"
	"github.com/gin-gonic/gin"
	"golang.org/x/crypto/bcrypt"
)

type UserLogin struct {
	UserName *string `json:"user_name"`
	Email    *string `json:"email"`
	Password string  `json:"password"`
}

// 出于幂等和不改变服务器状态的原则，login本应使用GET方法，但brave暂未支持https所以无法保证安全性
// 暂时使用POST方法传输
func Login(router *gin.RouterGroup) {

	router.POST("/login", func(c *gin.Context) {
		var ul UserLogin
		if err := c.BindJSON(&ul); err != nil {
			abort.AbortBadRequest(c)
			return
		}
		if err := verifyLoginParamter(ul); err != nil {
			abort.AbortBadRequest(c)
			return
		}

		user := &database.User{}
		userByName := &database.User{}
		userByEmail := &database.User{}
		if ul.UserName != nil {
			if err := userByName.GetByUserName(*ul.UserName); err != nil {
				log.Errorf("user %s (get by user_name): %v", *ul.UserName, err)
				log.Infof("user %s login failed", *ul.UserName)
				abort.AbortLoginError(c)
				return
			}
		}
		if ul.Email != nil {
			if err := userByEmail.GetByEmail(*ul.Email); err != nil {
				log.Errorf("user %s (get by email): %v", *ul.Email, err)
				log.Infof("user %s login failed", *ul.Email)
				abort.AbortLoginError(c)
				return
			}
		}
		if ul.UserName != nil && ul.Email == nil {
			user = userByName
		}
		if ul.UserName == nil && ul.Email != nil {
			user = userByEmail
		}
		if ul.UserName != nil && ul.Email != nil {
			user = userByName
			if userByName.UID != userByEmail.UID {
				log.Errorf("user %s incorrect", *ul.UserName)
				log.Infof("user %s login failed", *ul.UserName)
				abort.AbortUnexpected(c)
				return
			}
		}
		if err := bcrypt.CompareHashAndPassword(user.Password, []byte(ul.Password)); err != nil {
			log.Errorf("user %s Password incorrect", user.UID)
			log.Infof("user %s login failed", *ul.UserName)
			abort.AbortWrongPassword(c)
			return
		}

		// 校验用户是否登录
		ok, err := userLoginOrNot(user.UID)
		if err != nil {
			log.Infof("user %s login failed", *ul.UserName)
			abort.AbortUnexpected(c)
			return
		}
		if ok == nil {
			// 用户未登录
			log.Infof("user %s ready login", user.UID)
			userLogin()
		} else {
			// 用户已登录
			ser := loginServiceExistOrNot(ok.LoginHost)
			if ser == "" {
				log.Warnf("user %s login service has down", ok.UserId)
				userLogin()
			}

		}

		// 返回用户信息
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
			abort.AbortUnexpected(c)
			return
		}
		log.Infof("user %s login success", user.UID)
		c.JSON(http.StatusOK, resp)
	})
}

func verifyLoginParamter(ul UserLogin) error {
	if ul.UserName != nil {
		if err := VerifyUserName(*ul.UserName); err != nil {
			return err
		}
	}
	if ul.Email != nil {
		if err := VerifyEmail(*ul.Email); err != nil {
			return err
		}
	}
	if err := VerifyPassword(ul.Password); err != nil {
		return err
	}
	return nil
}

func userLoginOrNot(user_id string) (*etcd.User, error) {
	userFactory := etcd.UserFactory{
		Namespace: etcd.PrefixUsers,
		User:      &etcd.User{UserId: user_id},
	}
	if err := userFactory.Get(); err != nil {
		log.Errorf("get user %s from etcd error: %v", user_id, err)
		return nil, err
	}
	if userFactory.User.LoginTime == "" {
		log.Infof("user %s did not login", user_id)
		return nil, nil
	}
	return userFactory.User, nil
}

func loginServiceExistOrNot(host string) string {
	for _, v := range Services {
		if v == host {
			return host
		}
	}
	return ""
}

func userLogin() {
	// 随机获取一个service
	// 调用Joker接口登录
}
