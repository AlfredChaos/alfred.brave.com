package deliver

import (
	"context"
	"sync"
	"time"

	"alfred.brave.com/database"
)

// RouteTTL 路由缓存 TTL 上界 30s（D06 防线①：过期有上界，不依赖事件可靠）。
const RouteTTL = 30 * time.Second

// OnlineAddr online:{uid} 的解析结果。
type OnlineAddr struct {
	Cs   string `json:"cs"`
	Addr string `json:"addr"`
}

// routeEntry 缓存条目。
type routeEntry struct {
	addr     OnlineAddr
	expireAt time.Time
}

// Router 在线路由器：PG kv 权威 + 30s 进程内缓存。
// 并发保护：RWMutex 保护 map；缓存值不可变（整体替换）。
// 全系统路由副本方仅 deliver×N（D06：Joker 只查本机 sync.Map，不碰全局路由）。
// kvGetter Router 所需的最小 kv 读取能力（*database.KvStore 天然满足；测试可注入 fake）。
type kvGetter interface {
	Get(ctx context.Context, key string, out interface{}) error
}

type Router struct {
	kv    kvGetter
	mu    sync.RWMutex
	cache map[string]routeEntry
	nowFn func() time.Time // 可注入时钟（测试）
}

func NewRouter(kv kvGetter) *Router {
	return &Router{kv: kv, cache: make(map[string]routeEntry), nowFn: time.Now}
}

// Get 查 uid 的在线 CS gRPC 地址；离线返回 (zero, nil)——离线是正常判定不是错误（D12）。
func (r *Router) Get(ctx context.Context, uid string) (OnlineAddr, error) {
	r.mu.RLock()
	entry, ok := r.cache[uid]
	r.mu.RUnlock()
	if ok && r.nowFn().Before(entry.expireAt) {
		return entry.addr, nil
	}

	var addr OnlineAddr
	if err := r.kv.Get(ctx, "online:"+uid, &addr); err != nil {
		if err == database.ErrNotFound {
			// 缓存负结果也受 TTL 约束（addr 为空串表示离线）
			r.mu.Lock()
			r.cache[uid] = routeEntry{addr: OnlineAddr{}, expireAt: r.nowFn().Add(RouteTTL)}
			r.mu.Unlock()
			return OnlineAddr{}, nil
		}
		return OnlineAddr{}, err
	}
	r.mu.Lock()
	r.cache[uid] = routeEntry{addr: addr, expireAt: r.nowFn().Add(RouteTTL)}
	r.mu.Unlock()
	return addr, nil
}

// Invalidate 防线②：投递 not-found 时强制失效，下次查询直读权威值。
func (r *Router) Invalidate(uid string) {
	r.mu.Lock()
	delete(r.cache, uid)
	r.mu.Unlock()
}
