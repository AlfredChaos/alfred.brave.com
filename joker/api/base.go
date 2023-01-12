package api

import (
	"alfred.brave.com/event"
	"alfred.brave.com/joker/exchange"
)

var log = event.Log
var Manager = exchange.NewManager()
var ServiceId = ""
var ServiceHost = ""

func RegisterServiceId(serviceId string) {
	ServiceId = serviceId
}

func RegisterServiceHost(host string) {
	ServiceHost = host
}
