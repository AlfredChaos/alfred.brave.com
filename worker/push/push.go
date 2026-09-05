// Package push 离线通知 worker（§4 / D12）：消费 chat.notify → 节流判定 → 厂商通道（mock）。
// 演示级（dev-task §3 T16）：同人 60s 合并 + 单设备频控 20/h 的骨架逻辑真实实现并测试；
// 厂商通道不接真（LogVendor）；夜间静默不做（无客户端时区，标注遗留）。
package push

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"alfred.brave.com/event"
	"alfred.brave.com/internal/chat"

	"github.com/segmentio/kafka-go"
)

var log = event.Log

// 节流参数（§4 Push Worker 节流策略表）。
const (
	mergeWindow    = 60 * time.Second // 同人合并窗口
	deviceHourly   = 20               // 单设备每小时上限
	deviceTokenFmt = "device-%s"      // 演示级：无真实设备表，用 uid 派生 token
)

// state 单设备节流状态。仅 worker 单 goroutine 消费路径访问（无并发写）。
type deviceState struct {
	lastSentAt time.Time
	badge      int // 自上次发送后累计未播报的新消息数
	hourCount  int
	hourStart  time.Time
}

// Worker push 消费者。
type Worker struct {
	vendor  VendorChannel
	devices map[string]*deviceState
	reader  *kafka.Reader
	nowFn   func() time.Time
}

func New(vendor VendorChannel, brokers []string, groupID string) *Worker {
	return &Worker{
		vendor:  vendor,
		devices: make(map[string]*deviceState),
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers: brokers,
			GroupID: groupID,
			Topic:   notifyTopic,
		}),
		nowFn: time.Now,
	}
}

const notifyTopic = "chat.notify"

// Run 消费主循环：fetch → 节流判定 → flush（到期合并窗口）→ commit。
func (w *Worker) Run(ctx context.Context) error {
	log.Infof("push: consuming %s (group=%s)", notifyTopic, w.reader.Config().GroupID)
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Info("push: consumer stopped")
			return nil
		case <-ticker.C:
			w.flushDue(ctx)
		default:
		}

		fetchCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		m, err := w.reader.FetchMessage(fetchCtx)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				continue
			}
			if isTimeout(err) {
				continue // 3s 窗口无消息，回到 ticker 分支
			}
			return fmt.Errorf("push: fetch: %w", err)
		}
		var ev chat.Notify
		if err := json.Unmarshal(m.Value, &ev); err != nil {
			log.Errorf("push: bad payload: %v", err)
		} else {
			w.ingest(ctx, ev)
		}
		if err := w.reader.CommitMessages(ctx, m); err != nil {
			return err
		}
	}
}

// ingest 吸收一条通知（§4 同人合并）：
//   - 距上次发送 ≥ 60s（或首次）→ 立即发（避免全员 60s 延迟），badge 重置为 1
//   - 窗口内 → 只累加 badge，等 flushDue 到期合并播报
func (w *Worker) ingest(ctx context.Context, ev chat.Notify) {
	now := w.nowFn()
	st, ok := w.devices[ev.ToUID]
	if !ok {
		st = &deviceState{hourStart: now.Truncate(time.Hour)}
		w.devices[ev.ToUID] = st
	}
	if st.lastSentAt.IsZero() || now.Sub(st.lastSentAt) >= mergeWindow {
		st.badge = 1
		w.send(ctx, ev.ToUID, st)
		return
	}
	st.badge++
}

// flushDue 到期窗口合并播报：窗口内 badge>1（有未播报的累计）才重发一次。
func (w *Worker) flushDue(ctx context.Context) {
	now := w.nowFn()
	for uid, st := range w.devices {
		if st.badge > 1 && now.Sub(st.lastSentAt) >= mergeWindow {
			w.send(ctx, uid, st)
		}
	}
}

// send 发送（演示级）：频控判定 + 厂商通道（mock）。成功后 badge 清零、窗口重开。
func (w *Worker) send(ctx context.Context, uid string, st *deviceState) {
	now := w.nowFn()
	if now.Truncate(time.Hour) != st.hourStart {
		st.hourStart, st.hourCount = now.Truncate(time.Hour), 0
	}
	if st.hourCount >= deviceHourly {
		log.Warnf("push: device %s hourly limit reached, suppress (badge=%d)", uid, st.badge)
		return
	}
	title := "Brave IM"
	body := fmt.Sprintf("You have %d new message(s)", st.badge)
	if err := w.vendor.Push(ctx, fmt.Sprintf(deviceTokenFmt, uid), title, body, st.badge); err != nil {
		log.Errorf("push: vendor push to %s failed: %v", uid, err)
		return
	}
	st.lastSentAt = now
	st.hourCount++
	st.badge = 0
}

// isTimeout 判定 fetch 窗口超时。context.DeadlineExceeded 实现的是 Timeout() bool，
// 此前误断言 interface{ Deadline() bool } 恒不成立，空轮询超时被当致命错误上抛
// （旧版 worker 出错后僵尸化掩盖了这一点，见 commands/*Command.go 退出修复）。
func isTimeout(err error) bool {
	return errors.Is(err, context.DeadlineExceeded)
}
