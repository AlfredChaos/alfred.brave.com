package api

import (
	"net/http"
	"strings"

	"alfred.brave.com/event"
	"alfred.brave.com/internal/i18n"
	"github.com/gin-gonic/gin"
)

var log = event.Log

func Abort(c *gin.Context, code int, id i18n.Message, params ...interface{}) {
	resp := i18n.NewResponse(code, id, params...)

	log.Debugf("api-v1: abort %s with code %d (%s)", c.FullPath(), code, strings.ToLower(resp.String()))

	c.AbortWithStatusJSON(code, resp)
}

func AbortBadRequest(c *gin.Context) {
	Abort(c, http.StatusBadRequest, i18n.ErrBadRequest)
}

func AbortUnexpected(c *gin.Context) {
	Abort(c, http.StatusInternalServerError, i18n.ErrUnexpected)
}
