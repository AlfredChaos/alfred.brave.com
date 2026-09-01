package api

import (
	"encoding/json"
	"net/http"
	"strconv"

	"alfred.brave.com/database"
	"alfred.brave.com/internal/abort"
	"github.com/gin-gonic/gin"
)

type ConversationCreate struct {
	ToUID string `json:"to_uid"`
}

type ConversationResp struct {
	ConvID  string          `json:"conv_id"`
	Type    string          `json:"type"`
	Members json.RawMessage `json:"members"`
	LastSeq int64           `json:"last_seq"`
}

// Conversations 会话 API（鉴权保护）：建立/列出会话、按 seq 游标拉历史。
// 客户端补拉路径（D15 服务端半边）：重连/上线后 after_seq=本地最后 seq 增量拉取。
func (s *Server) Conversations(router *gin.RouterGroup) {
	auth := router.Group("", s.AuthRequired())

	auth.POST("/conversations", func(c *gin.Context) {
		uid := UidFrom(c)
		var body ConversationCreate
		if err := c.BindJSON(&body); err != nil || body.ToUID == "" {
			abort.AbortBadRequest(c)
			return
		}
		if body.ToUID == uid {
			abort.AbortBadRequest(c)
			return
		}
		if _, err := s.users.Get(c, body.ToUID); err != nil {
			log.Warnf("conversation target %s not found", body.ToUID)
			abort.AbortNotFound(c)
			return
		}
		conv, err := s.convs.EnsureSingle(c, uid, body.ToUID)
		if err != nil {
			log.Errorf("ensure conversation %s->%s: %v", uid, body.ToUID, err)
			abort.AbortDatabaseError(c)
			return
		}
		c.JSON(http.StatusOK, ConversationResp{
			ConvID: conv.ConvID, Type: conv.Type, Members: conv.Members, LastSeq: conv.LastSeq,
		})
	})

	auth.GET("/conversations", func(c *gin.Context) {
		convs, err := s.convs.ListByUser(c, UidFrom(c))
		if err != nil {
			log.Errorf("list conversations: %v", err)
			abort.AbortDatabaseError(c)
			return
		}
		result := make([]ConversationResp, 0, len(convs))
		for _, conv := range convs {
			result = append(result, ConversationResp{
				ConvID: conv.ConvID, Type: conv.Type, Members: conv.Members, LastSeq: conv.LastSeq,
			})
		}
		c.JSON(http.StatusOK, gin.H{"conversations": result})
	})

	auth.GET("/conversations/:id/messages", func(c *gin.Context) {
		uid := UidFrom(c)
		convID := c.Param("id")
		ok, err := s.convs.IsMember(c, convID, uid)
		if err != nil {
			log.Errorf("check membership: %v", err)
			abort.AbortDatabaseError(c)
			return
		}
		if !ok {
			abort.AbortUnauthorized(c)
			return
		}
		afterSeq, _ := strconv.ParseInt(c.DefaultQuery("after_seq", "0"), 10, 64)
		limit, _ := strconv.Atoi(c.DefaultQuery("limit", "50"))
		msgs, err := s.msgs.ListAfter(c, convID, afterSeq, limit)
		if err != nil {
			log.Errorf("list messages: %v", err)
			abort.AbortDatabaseError(c)
			return
		}
		c.JSON(http.StatusOK, gin.H{"messages": msgs})
	})
}

var _ = database.ErrNotFound // membership/用户查询经 store 抽象，database 仅用于类型
