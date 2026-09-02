// Package fanout 朋友圈扩散 worker（§6 步骤 7-9）：消费 feed.fanout →
// 取发布者好友列表 → 批量写 feed_inbox（500/批）→ big_v 跳过。
// 与 persist/deliver 同纪律：单 goroutine 消费，处理完才 commit（同发布者经 key=pub_uid 同分区串行）。
package fanout

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"alfred.brave.com/database"
	"alfred.brave.com/event"
	"alfred.brave.com/feed/api"

	"github.com/segmentio/kafka-go"
)

var log = event.Log

// batchSize inbox 批量写行数（§6 COPY 500/批；本实现用多值 INSERT 等价语义）。
const batchSize = 500

// Worker fanout 消费者。
type Worker struct {
	feeds   *database.FeedStore
	friends *friendLister
	reader  *kafka.Reader
}

// friendLister 好友 UID 列表获取（含 30s 本地缓存，§6 步骤 8）。
// 缓存仅 worker 单 goroutine 消费路径读写，无并发竞争。
type friendLister struct {
	friends *database.FriendStore
	cache   map[string]friendCacheEntry
}

type friendCacheEntry struct {
	uids     []string
	expireAt time.Time
}

func newFriendLister(f *database.FriendStore) *friendLister {
	return &friendLister{friends: f, cache: make(map[string]friendCacheEntry)}
}

func (fl *friendLister) list(ctx context.Context, uid string) ([]string, error) {
	if e, ok := fl.cache[uid]; ok && time.Now().Before(e.expireAt) {
		return e.uids, nil
	}
	friends, err := fl.friends.ListByOwner(ctx, uid)
	if err != nil {
		return nil, err
	}
	uids := make([]string, 0, len(friends))
	for _, f := range friends {
		uids = append(uids, f.FriendUID)
	}
	fl.cache[uid] = friendCacheEntry{uids: uids, expireAt: time.Now().Add(30 * time.Second)}
	return uids, nil
}

func New(store *database.Store, brokers []string, groupID string) *Worker {
	return &Worker{
		feeds:   database.NewFeedStore(store),
		friends: newFriendLister(database.NewFriendStore(store)),
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers: brokers,
			GroupID: groupID,
			Topic:   fanoutTopic,
		}),
	}
}

// fanoutTopic 由 feed/api 契约给定（此处避免 worker → feed/api 的包依赖，
// 直接使用 internal/chat 的常量）。
const fanoutTopic = "feed.fanout"

// RunCounters 计数聚合定时任务（T12 最小版：每 30s 重算近 5 分钟有动作的帖子）。
// 全量重算幂等，多实例重复执行无害。
func (w *Worker) RunCounters(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := w.FlushCounters(ctx, time.Now().Add(-5*time.Minute)); err != nil {
				log.Errorf("fanout: flush counters failed: %v", err)
			}
		}
	}
}

// FlushCounters 重算 since 之后有动作帖子的计数（测试可直接调用）。
func (w *Worker) FlushCounters(ctx context.Context, since time.Time) error {
	ids, err := w.feeds.TouchedPostIDs(ctx, since)
	if err != nil {
		return err
	}
	if len(ids) == 0 {
		return nil
	}
	if err := w.feeds.RecountPostCounters(ctx, ids); err != nil {
		return err
	}
	log.Debugf("fanout: recounted %d post counters", len(ids))
	return nil
}

// Run 消费主循环。
func (w *Worker) Run(ctx context.Context) error {
	log.Infof("fanout: consuming %s (group=%s)", fanoutTopic, w.reader.Config().GroupID)
	for {
		m, err := w.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				log.Info("fanout: consumer stopped")
				return nil
			}
			return fmt.Errorf("fanout: fetch: %w", err)
		}
		var ev api.FanoutEvent
		if err := json.Unmarshal(m.Value, &ev); err != nil {
			log.Errorf("fanout: bad payload: %v", err)
			if err := w.reader.CommitMessages(ctx, m); err != nil {
				return err
			}
			continue
		}
		if err := w.Handle(ctx, &ev); err != nil {
			log.Errorf("fanout: handle %s failed (retry): %v", ev.PostID, err)
			continue // 不 commit，等重投（写 inbox 幂等 ON CONFLICT DO NOTHING）
		}
		if err := w.reader.CommitMessages(ctx, m); err != nil {
			return err
		}
	}
}

// Handle 单条扩散（测试可直接调用）。
func (w *Worker) Handle(ctx context.Context, ev *api.FanoutEvent) error {
	// 帖子可能已被删除（发布到扩散窗口内删除）——tombstone 直接跳过
	posts, err := w.feeds.GetPosts(ctx, []string{ev.PostID})
	if err != nil {
		return err
	}
	post, ok := posts[ev.PostID]
	if !ok || post.IsDeleted {
		log.Warnf("fanout: post %s missing or deleted, skip", ev.PostID)
		return nil
	}
	// big_v 不写收件箱，读取时 pull（§6 hybrid 边界）
	if post.IsBigV {
		log.Debugf("fanout: post %s by big_v %s, skip inbox", ev.PostID, ev.PubUID)
		return nil
	}

	friends, err := w.friends.list(ctx, ev.PubUID)
	if err != nil {
		return err
	}
	createdAt := time.Unix(ev.CreatedAt, 0)
	written := 0
	for start := 0; start < len(friends); start += batchSize {
		end := start + batchSize
		if end > len(friends) {
			end = len(friends)
		}
		if err := w.feeds.InsertInboxBatch(ctx, friends[start:end], ev.PostID, createdAt); err != nil {
			return fmt.Errorf("inbox batch [%d:%d): %w", start, end, err)
		}
		written += end - start
	}
	log.Infof("fanout: post %s -> %d inboxes", ev.PostID, written)
	return nil
}
