package abort

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

func AbortNotFound(c *gin.Context) {
	Abort(c, http.StatusNotFound, i18n.ErrNotFound)
}

func AbortWrongPassword(c *gin.Context) {
	Abort(c, http.StatusUnauthorized, i18n.ErrPassword)
}

func AbortDatabaseError(c *gin.Context) {
	Abort(c, http.StatusInternalServerError, i18n.ErrDatabase)
}

func AbortLoginError(c *gin.Context) {
	Abort(c, http.StatusUnauthorized, i18n.ErrLogin)
}

func AbortUnauthorized(c *gin.Context) {
	Abort(c, http.StatusUnauthorized, i18n.ErrUnauthorized)
}

func AbortServiceUnavailable(c *gin.Context) {
	Abort(c, http.StatusServiceUnavailable, i18n.ErrServiceUnavailable)
}

func AbortForbidden(c *gin.Context) {
	Abort(c, http.StatusForbidden, i18n.ErrForbidden)
}

func AbortConflict(c *gin.Context) {
	Abort(c, http.StatusConflict, i18n.ErrConflict)
}
