package api

import (
	"math/rand"
	"net/http"

	"alfred.brave.com/database"
	"alfred.brave.com/internal/abort"
	"github.com/gin-gonic/gin"
	"golang.org/x/crypto/bcrypt"
)

type UserLogin struct {
	UserName *string `json:"user_name"`
	Email    *string `json:"email"`
	Password string  `json:"password"`
}

// Login 网关登录（§3 0a-0c）：bcrypt 校验 → 签发 token → 经 etcd 服务表选 CS →
// 下发 {token, ws_addr}。ws_addr 为 host:port，客户端拼 ws://ws_addr/ws/{uid} 直连。
// 出于幂等和不改变服务器状态的原则，login本应使用GET方法，但brave暂未支持https所以无法保证安全性
// 暂时使用POST方法传输
func (s *Server) Login(router *gin.RouterGroup) {

	router.POST("/login", func(c *gin.Context) {
		ul := &UserLogin{}

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

		// 经 etcd 服务表选 Chat Server；无可用节点时 503（§3 0b）
		wsAddr := getServiceByRandom()
		if wsAddr == "" {
			log.Error("no chat server available in etcd service table")
			abort.AbortServiceUnavailable(c)
			return
		}
		tokenStr, err := s.tokenizer.Sign(user.UID, TokenTTL)
		if err != nil {
			log.Errorf("sign token for %s failed: %v", user.UID, err)
			abort.AbortUnexpected(c)
			return
		}
		if err := s.users.UpdateLoginAt(c, user.UID); err != nil {
			log.Errorf("update login_at for %s failed: %v", user.UID, err)
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
			Token:     tokenStr,
			WsAddr:    wsAddr,
		}
		if err := s.AddFriends(c, resp); err != nil {
			abort.AbortUnexpected(c)
			return
		}
		log.Infof("user %s login success, ws_addr %s", user.UID, wsAddr)
		c.JSON(http.StatusOK, resp)
	})
}

// lookupLoginUser 按用户名/邮箱查用户；两者都给时必须指向同一账号。
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
				return nil, errMismatch
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
	return nil, errMismatch
}

var errMismatch = &loginParamError{}

type loginParamError struct{}

func (*loginParamError) Error() string { return "user_name and email point to different users" }

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
