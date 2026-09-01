package exchange

import (
	"context"
	"time"

	"alfred.brave.com/database"
)

// OnlineKV 在线状态的 kv 读写抽象（D06：Joker 是唯一写者）。
// 接口化便于单测注入 fake；生产实现 PgOnlineKV 包装 database.KvStore。
type OnlineKV interface {
	Upsert(ctx context.Context, uid, cs, addr string) error
	DeleteIfMatch(ctx context.Context, uid, cs string) error
}

// OnlineValue online:{uid} 的 kv 值结构。cs 与 etcd 服务表 value 同格式（host:port），
// GHOST 对账据此比对；addr 为该 CS 的 gRPC 投递地址（T06 deliver 使用）。
type OnlineValue struct {
	Cs   string `json:"cs"`
	Addr string `json:"addr"`
	Ts   int64  `json:"ts"`
}

// PgOnlineKV 基于 PG kv 表的默认实现。
type PgOnlineKV struct {
	kv *database.KvStore
}

func NewPgOnlineKV(kv *database.KvStore) *PgOnlineKV {
	return &PgOnlineKV{kv: kv}
}

func (p *PgOnlineKV) Upsert(ctx context.Context, uid, cs, addr string) error {
	return p.kv.Put(ctx, onlineKey(uid), OnlineValue{Cs: cs, Addr: addr, Ts: time.Now().Unix()})
}

// DeleteIfMatch 断开清理：仅当记录仍归属本 CS 时删除——重连漂移场景
// （用户已切到新 CS 并 upsert 新值）旧节点的晚到 del 不会误删新记录（§4 漂移窗口）。
func (p *PgOnlineKV) DeleteIfMatch(ctx context.Context, uid, cs string) error {
	return p.kv.DeleteIfMatch(ctx, onlineKey(uid), "cs", cs)
}

func onlineKey(uid string) string {
	return "online:" + uid
}
