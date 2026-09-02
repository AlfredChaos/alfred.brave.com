package api

import (
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"

	"github.com/gin-gonic/gin"
)

// FeedServices etcd 发现的 feed-api 地址表（server.StartListenFeedService 每 5s 刷新）。
var FeedServices = make([]string, 0)

// FeedProxy /v1/feed/* 透明转发（§2：网关只做鉴权+转发，feed-api 无状态任一副本可服务）。
// 实例随机挑选；鉴权透传（feed-api 与网关共享 secret，双端校验）。
func (s *Server) FeedProxy(router *gin.RouterGroup) {
	router.Any("/feed/*path", func(c *gin.Context) {
		if len(FeedServices) == 0 {
			c.JSON(http.StatusServiceUnavailable, gin.H{"code": 503, "error": "feed api unavailable"})
			return
		}
		target := FeedServices[rand.Intn(len(FeedServices))]
		base, err := url.Parse("http://" + target)
		if err != nil {
			c.JSON(http.StatusBadGateway, gin.H{"code": 502, "error": "bad feed upstream"})
			return
		}
		proxy := httputil.NewSingleHostReverseProxy(base)
		proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
			log.Errorf("feed proxy to %s failed: %v", target, err)
			w.WriteHeader(http.StatusBadGateway)
		}
		proxy.ServeHTTP(c.Writer, c.Request)
	})
}
