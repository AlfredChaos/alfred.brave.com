package exchange

import (
	"sync"
)

type Manager struct {
	Users       sync.Map
	Register    chan *Client
	Unregister  chan *Client
	Broadcast   chan []byte
	ServiceId   string
	ServiceHost string
}

func NewManager() *Manager {
	return &Manager{
		Users:      sync.Map{},
		Register:   make(chan *Client, 10000),
		Unregister: make(chan *Client, 10000),
		Broadcast:  make(chan []byte, 10000),
	}
}

func (manager *Manager) EventRegister(client *Client) {
	manager.Users.Store(client.UserId, client)
}

func (manager *Manager) EventUnregister(client *Client) {
	manager.Users.Delete(client.UserId)
}

func (manager *Manager) GetAllClients() []*Client {
	result := make([]*Client, 0)

	manager.Users.Range(func(k, v interface{}) bool {
		result = append(result, v.(*Client))
		return true
	})
	return result
}

func (manager *Manager) GetClient(userId string) *Client {
	v, _ := manager.Users.Load(userId)
	return v.(*Client)
}

func (manager *Manager) GetClientsLen() int {
	clients := manager.GetAllClients()
	return len(clients)
}

func (manager *Manager) Start() {
	for {
		select {
		case conn := <-manager.Register:
			manager.EventRegister(conn)

		case conn := <-manager.Unregister:
			manager.EventUnregister(conn)

		case message := <-manager.Broadcast:
			clients := manager.GetAllClients()
			for _, conn := range clients {
				select {
				case conn.Send <- message:
				default:
					close(conn.Send)
				}
			}
		}
	}
}
