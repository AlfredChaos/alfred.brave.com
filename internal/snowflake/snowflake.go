// Package snowflake 最小雪花 ID（D16）：41bit 毫秒时间戳 + 3bit worker + 10bit 序列。
// conv_id/gid 用它生成（唯一 + 趋势递增，为未来按 conv_id 分区留后门）。
// 时钟回拨保护：检测到回拨则自旋等待追平（本地部署可接受；跨 NTP 大回拨直接报错）。
package snowflake

import (
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"
)

const (
	workerBits = 3
	seqBits    = 10
	maxWorker  = -1 ^ (-1 << workerBits) // 7：三节点静态分配 0/1/2 绰绰有余
	maxSeq     = -1 ^ (-1 << seqBits)

	// epoch 自定义起点（2024-01-01），41bit 毫秒可用约 69 年。
	epoch int64 = 1704038400000
)

var (
	ErrClockBackward = errors.New("snowflake: clock moved backwards")
)

// Generator 单实例生成器。并发安全（互斥锁 + 内存位运算，开销可忽略）。
type Generator struct {
	mu     sync.Mutex
	worker int64
	lastMs int64
	seq    int64
}

func New(workerID int64) (*Generator, error) {
	if workerID < 0 || workerID > maxWorker {
		return nil, fmt.Errorf("snowflake: worker id %d out of range [0,%d]", workerID, maxWorker)
	}
	return &Generator{worker: workerID}, nil
}

// Next 生成一个 ID（十进制字符串，PG VARCHAR 存储）。
func (g *Generator) Next() (string, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	now := time.Now().UnixMilli()
	switch {
	case now < g.lastMs:
		// 回拨在 2ms 内：自旋等待（保护趋势递增）
		for wait := g.lastMs - now; wait > 0; wait = g.lastMs - time.Now().UnixMilli() {
			if time.Now().UnixMilli() >= g.lastMs {
				now = g.lastMs
				break
			}
			if g.lastMs-time.Now().UnixMilli() > 2 {
				return "", ErrClockBackward
			}
		}
		now = time.Now().UnixMilli()
		if now < g.lastMs {
			return "", ErrClockBackward
		}
	case now == g.lastMs:
		g.seq = (g.seq + 1) & maxSeq
		if g.seq == 0 { // 本毫秒序列耗尽，等下一毫秒
			for now <= g.lastMs {
				time.Sleep(50 * time.Microsecond)
				now = time.Now().UnixMilli()
			}
		}
	default:
		g.seq = 0
	}
	g.lastMs = now

	id := ((now - epoch) << (workerBits + seqBits)) | (g.worker << seqBits) | g.seq
	return strconv.FormatInt(id, 10), nil
}
