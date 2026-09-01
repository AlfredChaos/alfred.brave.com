package exchange

import (
	"encoding/json"
	"testing"
	"time"
)

// TestIdleExceeded 验证心跳超时判定：6 分钟无上报判超时，边界值刚好的不算。
// 对应 D14：客户端 30s 心跳 + 服务端 6 分钟超时清理。
func TestIdleExceeded(t *testing.T) {
	now := time.Now()
	timeout := 6 * time.Minute

	cases := []struct {
		name       string
		lastActive time.Time
		want       bool
	}{
		{"刚上报", now.Add(-30 * time.Second), false},
		{"5分钟前", now.Add(-5 * time.Minute), false},
		{"恰好6分钟", now.Add(-6 * time.Minute), true},
		{"超过6分钟", now.Add(-7 * time.Minute), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := idleExceeded(tc.lastActive, now, timeout); got != tc.want {
				t.Fatalf("idleExceeded() = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestManagerCleanIdleClients 验证清理任务只踢超时连接，活跃连接保留。
func TestManagerCleanIdleClients(t *testing.T) {
	m := NewManager()
	active := NewClient(m, "active", nil)
	idle := NewClient(m, "idle", nil)
	m.EventRegister(active)
	m.EventRegister(idle)

	// active 刚上报；idle 上报时间拨回 7 分钟前
	active.Touch()
	idle.TouchAt(time.Now().Add(-7 * time.Minute))

	kicked := m.cleanIdleClients(time.Now(), HeartbeatExpiration)
	if len(kicked) != 1 || kicked[0].UserId != "idle" {
		t.Fatalf("cleanIdleClients kicked = %v, want [idle]", kicked)
	}
	if m.GetClient("active") == nil {
		t.Fatal("active client should survive cleanup")
	}
	if m.GetClient("idle") != nil {
		t.Fatal("idle client should be removed from manager")
	}
	// Send 通道被关闭（通知 WritePump 退出）
	select {
	case _, ok := <-idle.Send:
		if ok {
			t.Fatal("idle client Send chan should be closed")
		}
	default:
		t.Fatal("idle client Send chan should be closed, not blocked")
	}
}

// TestClientCloseIdempotent Close 幂等：重复调用不 panic（sweeper 与 Pump 退出路径可能并发触发）。
func TestClientCloseIdempotent(t *testing.T) {
	c := NewClient(NewManager(), "u1", nil)
	c.Close()
	c.Close() // 第二次调用不得 panic（close of closed channel）
}

// TestHeartbeatRefreshLastActive 心跳处理必须刷新 lastActive 时间戳（D14）。
func TestHeartbeatRefreshLastActive(t *testing.T) {
	c := NewClient(NewManager(), "u1", nil)
	old := time.Now().Add(-7 * time.Minute)
	c.TouchAt(old)
	if !idleExceeded(lastActiveOf(c), time.Now(), HeartbeatExpiration) {
		t.Fatal("precondition: client should be idle before heartbeat")
	}
	handleHeartbeat(c, json.RawMessage(`{}`))
	if idleExceeded(lastActiveOf(c), time.Now(), HeartbeatExpiration) {
		t.Fatal("heartbeat should refresh lastActive")
	}
}

// lastActiveOf 测试辅助：读取客户端当前活跃时间。
func lastActiveOf(c *Client) time.Time {
	return time.Unix(0, c.lastActive.Load())
}
