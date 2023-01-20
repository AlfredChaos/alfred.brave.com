package common

import "alfred.brave.com/joker/exchange"

var Manager = exchange.NewManager()
var ServiceId = ""
var ServiceHost = ""

func RegisterServiceId(serviceId string) {
	ServiceId = serviceId
}

func RegisterServiceHost(host string) {
	ServiceHost = host
}
