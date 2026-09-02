-- +goose Up
-- 朋友圈域四表（§7）。
-- 设计要点：
--   * inbox 只存 (uid, post_id) 引用，push 为主；big_v 发布跳过 fanout，读取时 pull（§6）
--   * posts 软删 tombstone（is_deleted），inbox 不物理清理，读取时过滤
--   * counters 异步聚合落库（T12 定时 flush），post_actions 是事实源

CREATE TABLE posts (
    post_id    VARCHAR(36)  PRIMARY KEY,
    uid        VARCHAR(36)  NOT NULL,
    content    JSONB        NOT NULL,
    media      JSONB        NOT NULL DEFAULT '[]'::jsonb,
    is_big_v   BOOLEAN      NOT NULL DEFAULT false,
    is_deleted BOOLEAN      NOT NULL DEFAULT false,
    created_at TIMESTAMPTZ  NOT NULL DEFAULT now()
);
-- big_v pull：按作者取最近 N 条
CREATE INDEX idx_posts_uid_created ON posts (uid, created_at DESC);

CREATE TABLE feed_inbox (
    uid        VARCHAR(36) NOT NULL,
    post_id    VARCHAR(36) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (uid, post_id)
);
-- 收件箱游标分页：按时间倒序
CREATE INDEX idx_feed_inbox_uid_time ON feed_inbox (uid, created_at DESC);

CREATE TABLE post_actions (
    post_id    VARCHAR(36) NOT NULL,
    uid        VARCHAR(36) NOT NULL,
    action     VARCHAR(16) NOT NULL CHECK (action IN ('like', 'comment')),
    content    JSONB       NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (post_id, uid, action)
);
-- 计数聚合扫描
CREATE INDEX idx_post_actions_time ON post_actions (created_at);

CREATE TABLE post_counters (
    post_id     VARCHAR(36) PRIMARY KEY,
    like_cnt    BIGINT NOT NULL DEFAULT 0,
    comment_cnt BIGINT NOT NULL DEFAULT 0,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- +goose Down
DROP TABLE IF EXISTS post_counters;
DROP TABLE IF EXISTS post_actions;
DROP TABLE IF EXISTS feed_inbox;
DROP TABLE IF EXISTS posts;
