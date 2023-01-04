package exchange

import "sync"

type Manager struct {
	Users      sync.Map
	Register   chan *Client
	Unregister chan *Client
	Broadcast  chan []byte
}

func NewManager() *Manager {
	return &Manager{
		Users:      sync.Map{},
		Register:   make(chan *Client, 10000),
		Unregister: make(chan *Client, 10000),
		Broadcast:  make(chan []byte, 10000),
	}
}
