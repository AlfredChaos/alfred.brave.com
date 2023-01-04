package api

import (
	"alfred.brave.com/event"
	"alfred.brave.com/joker/exchange"
)

var log = event.Log
var Manager = exchange.NewManager()
