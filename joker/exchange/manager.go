package exchange

import (
	"sync"
	"time"
)

// Manager 连接表与连接生命周期事件循环。
// Users 为本机权威连接子集（sync.Map 并发安全）；清理任务每分钟扫一次超时连接。
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

// EventUnregister 从连接表移除；重复移除（清理任务先踢 + Pump 退出再报）幂等无害。
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

// GetClient 返回本机连接；不存在时返回 nil（原实现类型断言会 panic）。
func (manager *Manager) GetClient(userId string) *Client {
	v, ok := manager.Users.Load(userId)
	if !ok {
		return nil
	}
	return v.(*Client)
}

func (manager *Manager) GetClientsLen() int {
	clients := manager.GetAllClients()
	return len(clients)
}

// idleExceeded 心跳超时判定（D14）：now 与最近活跃时间差超过阈值即超时。
func idleExceeded(lastActive time.Time, now time.Time, timeout time.Duration) bool {
	return now.Sub(lastActive) >= timeout
}

// cleanIdleClients 清理超时连接：踢线（幂等 Close）+ 移出连接表，返回被踢连接列表。
// 逐个 Close 而非批量 close Send：Close 内部同时关 Socket，保证两个 Pump 都能退出。
func (manager *Manager) cleanIdleClients(now time.Time, timeout time.Duration) []*Client {
	kicked := make([]*Client, 0)
	for _, client := range manager.GetAllClients() {
		last := time.Unix(0, client.lastActive.Load())
		if !idleExceeded(last, now, timeout) {
			continue
		}
		log.Warnf("user %s heartbeat expired (last active %s), kicking", client.UserId, last.Format(time.RFC3339))
		client.Close()
		manager.EventUnregister(client)
		kicked = append(kicked, client)
	}
	return kicked
}

func (manager *Manager) Start() {
	// 心跳清理任务（D14）：1 分钟扫一次，6 分钟无心跳踢线
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
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
					// Send 满 = 客户端消费不动，按 D20 环节④踢线不跳跃
					conn.Close()
				}
			}

		case now := <-ticker.C:
			manager.cleanIdleClients(now, HeartbeatExpiration)
		}
	}
}
