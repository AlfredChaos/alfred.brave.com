package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"alfred.brave.com/database"
	"alfred.brave.com/internal/abort"
	"alfred.brave.com/internal/chat"

	"github.com/gin-gonic/gin"
)

type GroupCreate struct {
	Name    string   `json:"name"`
	Members []string `json:"members"`
}

type MembersAdd struct {
	UIDs []string `json:"uids"`
}

type GroupRenameReq struct {
	Name string `json:"name"`
}

type GroupAnnounceReq struct {
	Text string `json:"text"`
}

type GroupPinReq struct {
	MsgID string `json:"msg_id"`
}

// Groups 群管理 API（§5 管理面，不碰数据面）。owner-only 权限矩阵在 GroupStore 事务内权威校验，
// API 层只做身份透传与错误映射。每次管理操作成功后把预写的 system_event 消息投进 chat.msg
// （persist 幂等跳过落库、统一扇出，D09/D19 单一扇出路径）。
func (s *Server) Groups(router *gin.RouterGroup) {
	auth := router.Group("", s.AuthRequired())

	auth.POST("/groups", func(c *gin.Context) {
		uid := UidFrom(c)
		var body GroupCreate
		if err := c.BindJSON(&body); err != nil || body.Name == "" {
			abort.AbortBadRequest(c)
			return
		}
		gid, eventMsgID, err := s.groups.CreateGroup(c, s.gidGen, uid, body.Name, body.Members)
		s.respondGroupMutation(c, gid, uid, eventMsgID, err, gin.H{"gid": gid})
	})

	auth.GET("/groups", func(c *gin.Context) {
		groups, err := s.groups.ListGroupsByUser(c, UidFrom(c))
		if err != nil {
			abort.AbortDatabaseError(c)
			return
		}
		c.JSON(http.StatusOK, gin.H{"groups": groups})
	})

	auth.GET("/groups/:gid", func(c *gin.Context) {
		g, err := s.groups.GetGroup(c, c.Param("gid"))
		if err != nil {
			abort.AbortNotFound(c)
			return
		}
		members, err := s.groups.ActiveMembers(c, g.GID)
		if err != nil {
			abort.AbortDatabaseError(c)
			return
		}
		c.JSON(http.StatusOK, gin.H{"group": g, "members": members})
	})

	auth.POST("/groups/:gid/members", func(c *gin.Context) {
		uid := UidFrom(c)
		var body MembersAdd
		if err := c.BindJSON(&body); err != nil || len(body.UIDs) == 0 {
			abort.AbortBadRequest(c)
			return
		}
		msgID, err := s.groups.AddMembers(c, c.Param("gid"), uid, body.UIDs)
		s.respondGroupMutation(c, c.Param("gid"), uid, msgID, err, gin.H{"added": true})
	})

	auth.DELETE("/groups/:gid/members/:uid", func(c *gin.Context) {
		uid := UidFrom(c)
		msgID, err := s.groups.RemoveMember(c, c.Param("gid"), uid, c.Param("uid"))
		s.respondGroupMutation(c, c.Param("gid"), uid, msgID, err, gin.H{"kicked": c.Param("uid")})
	})

	auth.POST("/groups/:gid/quit", func(c *gin.Context) {
		uid := UidFrom(c)
		msgID, err := s.groups.Quit(c, c.Param("gid"), uid)
		s.respondGroupMutation(c, c.Param("gid"), uid, msgID, err, gin.H{"quit": true})
	})

	auth.PATCH("/groups/:gid", func(c *gin.Context) {
		uid := UidFrom(c)
		var body GroupRenameReq
		if err := c.BindJSON(&body); err != nil || body.Name == "" {
			abort.AbortBadRequest(c)
			return
		}
		msgID, err := s.groups.Rename(c, c.Param("gid"), uid, body.Name)
		s.respondGroupMutation(c, c.Param("gid"), uid, msgID, err, gin.H{"renamed": body.Name})
	})

	auth.PUT("/groups/:gid/announcement", func(c *gin.Context) {
		uid := UidFrom(c)
		var body GroupAnnounceReq
		if err := c.BindJSON(&body); err != nil {
			abort.AbortBadRequest(c)
			return
		}
		msgID, err := s.groups.SetAnnouncement(c, c.Param("gid"), uid, body.Text)
		s.respondGroupMutation(c, c.Param("gid"), uid, msgID, err, gin.H{"announced": true})
	})

	auth.PUT("/groups/:gid/pinned", func(c *gin.Context) {
		uid := UidFrom(c)
		var body GroupPinReq
		if err := c.BindJSON(&body); err != nil {
			abort.AbortBadRequest(c)
			return
		}
		msgID, err := s.groups.SetPin(c, c.Param("gid"), uid, body.MsgID)
		s.respondGroupMutation(c, c.Param("gid"), uid, msgID, err, gin.H{"pinned": body.MsgID})
	})

	auth.DELETE("/groups/:gid", func(c *gin.Context) {
		uid := UidFrom(c)
		msgID, err := s.groups.Dismiss(c, c.Param("gid"), uid)
		s.respondGroupMutation(c, c.Param("gid"), uid, msgID, err, gin.H{"dismissed": true})
	})
}

// respondGroupMutation 统一处理管理操作结果：错误映射 → 成功则投递群事件到 chat.msg。
func (s *Server) respondGroupMutation(c *gin.Context, gid, actor, eventMsgID string, err error, payload gin.H) {
	if err != nil {
		switch {
		case errors.Is(err, database.ErrNotOwner), errors.Is(err, database.ErrOwnerCantQuit):
			abort.AbortForbidden(c)
		case errors.Is(err, database.ErrGroupFull), errors.Is(err, database.ErrGroupDismissed):
			abort.AbortConflict(c)
		case errors.Is(err, database.ErrBadTarget), errors.Is(err, database.ErrNotMember):
			abort.AbortBadRequest(c)
		case errors.Is(err, database.ErrNotFound):
			abort.AbortNotFound(c)
		default:
			log.Errorf("group mutation gid=%s actor=%s: %v", gid, actor, err)
			abort.AbortDatabaseError(c)
		}
		return
	}
	s.emitGroupEvent(c, gid, actor, eventMsgID)
	payload["gid"] = gid
	c.JSON(http.StatusOK, payload)
}

// emitGroupEvent 把预写的 system_event 消息投进 chat.msg（key=gid 保序；persist 幂等扇出）。
// produce 失败不影响管理操作本身（消息已落库，成员打开会话按 seq 可见，损失的是实时推）。
func (s *Server) emitGroupEvent(c *gin.Context, gid, actor, msgID string) {
	if msgID == "" || s.chatMsg == nil {
		return
	}
	env := &chat.Msg{
		MsgID:   msgID, // 预写消息 ID：persist 检测到已存在即跳过 INSERT 只扇出
		ConvID:  gid,
		FromUID: actor,
		Type:    chat.TypeSystemEvent,
		Content: map[string]interface{}{},
		SentAt:  time.Now().Unix(),
	}
	raw, err := json.Marshal(env)
	if err != nil {
		log.Errorf("marshal group event %s: %v", msgID, err)
		return
	}
	ctx, cancel := context.WithTimeout(c, 5*time.Second)
	defer cancel()
	if err := s.chatMsg.Write(ctx, gid, raw); err != nil {
		log.Errorf("produce group event %s to chat.msg failed (message persisted, realtime push lost): %v", msgID, err)
	}
}
