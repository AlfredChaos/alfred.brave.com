package i18n

import (
	"errors"
	"fmt"
	"strings"

	"alfred.brave.com/event"
)

var log = event.Log

type Message int
type MessageMap map[Message]string

// msgParams replaces message params with the actual values.
func msgParams(msg string, params ...interface{}) string {
	if strings.Contains(msg, "%") {
		msg = fmt.Sprintf(msg, params...)
	}

	return msg
}

func Msg(id Message, params ...interface{}) string {
	msg, ok := Messages[id]
	if !ok {
		log.Errorf("response message %v not register", id)
	}
	return msgParams(msg, params...)
}

func Error(id Message, params ...interface{}) error {
	return errors.New(Msg(id, params...))
}
