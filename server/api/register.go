package api

import (
	"errors"
	"net/http"
	"net/mail"
	"strings"
	"time"

	"alfred.brave.com/database"
	"alfred.brave.com/internal/abort"
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
	UID string `json:"uid"`
	// 返回值是RFC3339格式，例如2022-12-26T14:35:03+08:00
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
	LoginAt   time.Time `json:"login_at"`
	LoginHost string    `json:"login_host"`
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

func (s *Server) Register(router *gin.RouterGroup) {
	router.POST("/register", func(c *gin.Context) {
		var ur UserRegister
		if err := c.BindJSON(&ur); err != nil {
			abort.AbortBadRequest(c)
			return
		}

		if err := verifyRegisterParamter(ur); err != nil {
			abort.AbortBadRequest(c)
			return
		}
		hash, err := bcrypt.GenerateFromPassword([]byte(ur.Password), bcrypt.DefaultCost)
		if err != nil {
			log.Errorf("generate from password fail: %v", err)
			abort.AbortBadRequest(c)
			return
		}
		user := &database.User{
			UserName: ur.UserName,
			Email:    ur.Email,
			Profile:  ur.Profile,
			Password: hash,
			LoginAt:  time.Now(),
		}
		if err := s.users.Create(c, user); err != nil {
			log.Errorf("user %s (create): %v", ur.UserName, err)
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
		c.JSON(http.StatusOK, resp)
	})
}

func verifyRegisterParamter(ur UserRegister) error {
	if err := VerifyUserName(ur.UserName); err != nil {
		return err
	}
	if err := VerifyEmail(ur.Email); err != nil {
		return err
	}
	if err := VerifyPassword(ur.Password); err != nil {
		return err
	}
	return nil
}

// VerifyUserName 用户名校验：3-32 字符，仅允许字母/数字/下划线/连字符/中文。
// 返回错误由调用方统一走 AbortBadRequest（i18n）。
func VerifyUserName(name string) error {
	runeLen := len([]rune(name))
	if runeLen < 3 || runeLen > 32 {
		return errors.New("user_name length must be 3-32")
	}
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		case r == '_' || r == '-':
		case r >= 0x4e00 && r <= 0x9fa5: // 中文
		default:
			return errors.New("user_name contains illegal character")
		}
	}
	return nil
}

// VerifyEmail 邮箱校验：RFC 5322 可解析、含 @、总长 ≤254。
func VerifyEmail(email string) error {
	if len(email) == 0 || len(email) > 254 {
		return errors.New("email length must be 1-254")
	}
	addr, err := mail.ParseAddress(email)
	if err != nil {
		return errors.New("email format illegal")
	}
	if addr.Address != email || !strings.Contains(email, "@") {
		return errors.New("email format illegal")
	}
	return nil
}

// VerifyPassword 密码校验：8-64 字符，至少包含一个字母和一个数字。
// 不强制特殊字符——练手项目在可讲与可用之间取简（bcrypt 负责存储安全）。
func VerifyPassword(password string) error {
	if len(password) < 8 || len(password) > 64 {
		return errors.New("password length must be 8-64")
	}
	hasLetter, hasDigit := false, false
	for _, r := range password {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z':
			hasLetter = true
		case r >= '0' && r <= '9':
			hasDigit = true
		}
	}
	if !hasLetter || !hasDigit {
		return errors.New("password must contain at least one letter and one digit")
	}
	return nil
}
