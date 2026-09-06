// Package ghost 在线状态对账任务（§4 GHOST 清理）：
// 定时扫描 PG kv online:*，比对 etcd 存活服务表，清理“kv 在、CS 已下线”的残留记录。
// 窗口最坏 = lease 过期(60s) + 扫描周期(60s) ≈ 2 分钟；期间的错误投递由 deliver 的
// not-found 即时校正兜底（D06 防线②，T06 实现）。
package ghost

import (
	"context"
	"encoding/json"
	"time"

	"alfred.brave.com/database"
	"alfred.brave.com/event"
)

var log = event.Log

// kvAccess 对账所需的最小 kv 能力（接口化便于单测）。
type kvAccess interface {
	ScanPrefix(ctx context.Context, prefix, afterKey string, limit int) ([]database.KvEntry, error)
	DeleteIfMatch(ctx context.Context, key, field, self string) error
}

// Reconciler GHOST 对账器。aliveFn 返回当前存活 CS 集合（来自 etcd 服务发现）。
type Reconciler struct {
	kv       kvAccess
	aliveFn  func() map[string]bool
	interval time.Duration
	pageSize int // 扫描分页行数（单测可调小触发多页路径）
}

// NewReconciler interval 默认 60s；单测可改字段。
func NewReconciler(kv kvAccess, aliveFn func() map[string]bool) *Reconciler {
	return &Reconciler{kv: kv, aliveFn: aliveFn, interval: 60 * time.Second, pageSize: 5000}
}

// onlineCs 从 kv 值解析 cs 标识。
func onlineCs(raw json.RawMessage) (string, error) {
	var v struct {
		Cs string `json:"cs"`
	}
	if err := json.Unmarshal(raw, &v); err != nil {
		return "", err
	}
	return v.Cs, nil
}

// Sweep 执行一轮对账，返回清理条数。解析失败的记录跳过并告警（不误删）。
// 扫描按 key 游标分页循环，覆盖任意规模的 online 表。
func (r *Reconciler) Sweep(ctx context.Context) (int, error) {
	alive := r.aliveFn()
	removed := 0
	afterKey := ""
	for {
		entries, err := r.kv.ScanPrefix(ctx, "online:", afterKey, r.pageSize)
		if err != nil {
			return removed, err
		}
		for _, e := range entries {
			cs, err := onlineCs(e.Value)
			if err != nil {
				log.Warnf("ghost: skip malformed online record %s: %v", e.Key, err)
				continue
			}
			if alive[cs] {
				continue
			}
			// cs 已下线：GHOST 记录，删除（用户重连会重新 upsert）
			if err := r.kv.DeleteIfMatch(ctx, e.Key, "cs", cs); err != nil {
				log.Errorf("ghost: delete %s failed: %v", e.Key, err)
				continue
			}
			log.Infof("ghost: removed stale online record %s (cs %s down)", e.Key, cs)
			removed++
		}
		if len(entries) < r.pageSize {
			return removed, nil // 末页不足一页 → 扫尽
		}
		afterKey = entries[len(entries)-1].Key
	}
}

// Run 常驻循环：按 interval 周期对账，随 context 取消退出。
func (r *Reconciler) Run(ctx context.Context) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	log.Infof("ghost: reconciler started, interval %s", r.interval)
	for {
		select {
		case <-ctx.Done():
			log.Info("ghost: reconciler stopped")
			return
		case <-ticker.C:
			removed, err := r.Sweep(ctx)
			if err != nil {
				log.Errorf("ghost: sweep failed: %v", err)
				continue
			}
			if removed > 0 {
				log.Infof("ghost: swept %d stale records", removed)
			}
		}
	}
}
