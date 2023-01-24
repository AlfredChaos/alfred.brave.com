package exchange

var ServiceId = ""
var ServiceHost = ""
var Controller *Manager = NewManager()

func RegisterServiceId(serviceId string) {
	ServiceId = serviceId
}

func RegisterServiceHost(host string) {
	ServiceHost = host
}
