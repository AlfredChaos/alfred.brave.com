package http_client

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/gin-gonic/gin"
)

const (
	LoginPath  = "/login"
	LogoutPath = "/logout"
)

type JokenClient struct {
	URL *URL
}

type UserLogin struct {
	UserId    string `json:"user_id"`
	UserToken string `json:"user_token"`
	LoginTime string `json:"login_time"`
}

func NewJokenClient(host string) JokenClient {
	if !strings.Contains(host, "//") {
		host = fmt.Sprintf("http://%s", host)
	}
	return JokenClient{URL: &URL{Base: host}}
}

func (j *JokenClient) Login(c *gin.Context, body UserLogin, query map[string]string, variables ...string) error {
	j.URL.Combind(LoginPath, query, variables...)
	bodyBytes, _ := json.Marshal(body)
	if _, err := Post(c, j.URL, bodyBytes, nil); err != nil {
		return err
	}
	return nil
}
