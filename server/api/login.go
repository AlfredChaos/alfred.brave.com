package api

import (
	"errors"
	"math/rand"
	"net/http"

	"alfred.brave.com/common"
	"alfred.brave.com/database"
	"alfred.brave.com/internal/abort"
	"alfred.brave.com/internal/etcd"
	"alfred.brave.com/internal/http_client"
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
// 注：T03 网关化将把本端点改造为返回 {token, ws_addr}，etcd 登录态随 T04 迁移到 PG kv。
func (s *Server) Login(router *gin.RouterGroup) {

	router.POST("/login", func(c *gin.Context) {
		ul := &UserLogin{}
		resp := &UserResponse{}

		if err := c.BindJSON(&ul); err != nil {
			abort.AbortBadRequest(c)
			return
		}
		if err := verifyLoginParamter(ul); err != nil {
			abort.AbortBadRequest(c)
			return
		}

		user, err := s.lookupLoginUser(c, ul)
		if err != nil {
			log.Infof("user login failed (lookup): %v", err)
			abort.AbortLoginError(c)
			return
		}
		if err := bcrypt.CompareHashAndPassword(user.Password, []byte(ul.Password)); err != nil {
			log.Errorf("user %s password incorrect", user.UID)
			abort.AbortWrongPassword(c)
			return
		}

		// 校验用户是否登录
		ok, err := userLoginOrNot(user.UID)
		if err != nil {
			log.Infof("user %s login failed", user.UID)
			abort.AbortUnexpected(c)
			return
		}
		if ok == nil {
			// 用户未登录
			log.Infof("user %s ready login", user.UID)
			if err := userLogin(c, user); err != nil {
				log.Errorf("user %s login failed", user.UID)
				abort.AbortLoginError(c)
				return
			}
		} else {
			// 用户已登录
			ser := loginServiceExistOrNot(ok.LoginHost)
			if ser == "" {
				log.Warnf("user %s login service has down", ok.UserId)
				if err := userLogin(c, user); err != nil {
					log.Errorf("user %s login failed", user.UID)
					abort.AbortLoginError(c)
					return
				}
			}
		}
		loginUser, err := userLoginOrNot(user.UID)
		if err != nil {
			log.Infof("user %s login failed", user.UID)
			abort.AbortUnexpected(c)
			return
		}

		resp.UID = user.UID
		resp.CreatedAt = user.CreatedAt
		resp.UpdatedAt = user.UpdatedAt
		resp.LoginAt = user.LoginAt
		resp.UserName = user.UserName
		resp.Email = user.Email
		resp.Profile = user.Profile
		resp.Avatar = user.Avatar
		resp.Friends = make([]Friend, 0)
		resp.LoginHost = loginUser.LoginHost
		if err := s.AddFriends(c, resp); err != nil {
			abort.AbortUnexpected(c)
			return
		}
		log.Infof("user %s login success", user.UID)
		c.JSON(http.StatusOK, resp)
	})
}

// lookupLoginUser 按用户名/邮箱查用户；两者都给时必须指向同一账号（对齐原版语义）。
func (s *Server) lookupLoginUser(c *gin.Context, ul *UserLogin) (*database.User, error) {
	if ul.UserName != nil {
		user, err := s.users.GetByUserName(c, *ul.UserName)
		if err != nil {
			log.Errorf("user %s (get by user_name): %v", *ul.UserName, err)
			return nil, err
		}
		if ul.Email != nil {
			byEmail, err := s.users.GetByEmail(c, *ul.Email)
			if err != nil {
				log.Errorf("user %s (get by email): %v", *ul.Email, err)
				return nil, err
			}
			if user.UID != byEmail.UID {
				return nil, errors.New("user_name/email required")
			}
		}
		return user, nil
	}
	if ul.Email != nil {
		user, err := s.users.GetByEmail(c, *ul.Email)
		if err != nil {
			log.Errorf("user %s (get by email): %v", *ul.Email, err)
			return nil, err
		}
		return user, nil
	}
	return nil, errors.New("user_name/email required")
}

func verifyLoginParamter(ul *UserLogin) error {
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
	return VerifyPassword(ul.Password)
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

func userLogin(c *gin.Context, user *database.User) error {
	// 随机获取一个service
	service := getServiceByRandom()
	// 调用Joker接口登录
	ul := http_client.UserLogin{
		UserId:    user.UID,
		LoginTime: user.LoginAt.Format(common.TimeFormat),
	}
	jokerClient := http_client.NewJokenClient(service)
	if err := jokerClient.Login(c, ul, QueryParams(c)); err != nil {
		log.Errorf("Joker http client login failed, err = %v", err)
		return err
	}
	return nil
}

func QueryParams(c *gin.Context) map[string]string {
	res := make(map[string]string)
	return res
}

func getServiceByRandom() string {
	serviceNum := len(Services)
	// 服务发现列表可能为空（joker 尚未注册完成），rand.Intn(0) 会 panic。
	// 返回空串由调用方作为“无可用节点”错误处理，避免进程崩溃。
	if serviceNum == 0 {
		return ""
	}
	// Go 1.20+ 全局随机源已自动播种；显式 rand.Seed 并发不安全且已被废弃。
	return Services[rand.Intn(serviceNum)]
}
