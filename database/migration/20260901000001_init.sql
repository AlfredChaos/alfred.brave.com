-- +goose Up
-- 初始 schema：聊天域六表（对齐 docs/target-architecture.html §7）。
-- 设计要点：
--   * messages 不分区，UNIQUE(conv_id, seq) 全局有效（D16 幂等防线）
--   * kv 为 JSONB 通用 KV：online:{uid} / seq:{conv_id} / feedcur:{uid}（D05/D06）
--   * friends 双边两行存储（D07）

CREATE TABLE users (
    uid            VARCHAR(36)  PRIMARY KEY,
    user_name      VARCHAR(64)  NOT NULL,
    email          VARCHAR(254) NOT NULL,
    password_hash  BYTEA        NOT NULL,
    profile        VARCHAR(255) NOT NULL DEFAULT '',
    avatar         BYTEA,
    login_at       TIMESTAMPTZ,
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT uq_users_user_name UNIQUE (user_name),
    CONSTRAINT uq_users_email     UNIQUE (email)
);

CREATE TABLE friends (
    owner_uid  VARCHAR(36) NOT NULL,
    friend_uid VARCHAR(36) NOT NULL,
    status     SMALLINT    NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (owner_uid, friend_uid)
);
-- 反向查询（谁加了我 / 好友去重判定）
CREATE INDEX idx_friends_friend_uid ON friends (friend_uid);

-- conversations：单聊与群聊共用一张会话表；群聊 conv_id = gid。
-- single_key 为单聊规范化成员对（uid 小:大），唯一约束防止 A→B/B→A 重复建会话；群聊为 NULL。
CREATE TABLE conversations (
    conv_id    VARCHAR(64)  PRIMARY KEY,
    type       VARCHAR(16)  NOT NULL CHECK (type IN ('single', 'group')),
    single_key VARCHAR(141) UNIQUE,
    members    JSONB        NOT NULL DEFAULT '[]'::jsonb,
    last_seq   BIGINT       NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX idx_conversations_members ON conversations USING GIN (members);

CREATE TABLE conversation_members (
    conv_id       VARCHAR(64) NOT NULL,
    uid           VARCHAR(36) NOT NULL,
    last_read_seq BIGINT      NOT NULL DEFAULT 0,
    muted         BOOLEAN     NOT NULL DEFAULT false,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (conv_id, uid)
);
CREATE INDEX idx_conversation_members_uid ON conversation_members (uid);

CREATE TABLE messages (
    msg_id     VARCHAR(36)  PRIMARY KEY,
    conv_id    VARCHAR(64)  NOT NULL,
    seq        BIGINT       NOT NULL,
    from_uid   VARCHAR(36)  NOT NULL,
    type       VARCHAR(16)  NOT NULL DEFAULT 'single' CHECK (type IN ('single', 'group', 'system_event')),
    content    JSONB        NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ  NOT NULL DEFAULT now(),
    -- 会话内序号唯一：重放/并发兜底（D08/D16）
    CONSTRAINT uq_messages_conv_seq UNIQUE (conv_id, seq)
);
-- 历史拉取主路径：按会话倒序分页
CREATE INDEX idx_messages_conv_seq_desc ON messages (conv_id, seq DESC);

CREATE TABLE kv (
    k          TEXT        PRIMARY KEY,
    v          JSONB       NOT NULL,
    version    BIGINT      NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- GHOST 对账等前缀扫描（online:*）走 text_pattern_ops 索引
CREATE INDEX idx_kv_k_pattern ON kv (k text_pattern_ops);

-- +goose Down
DROP TABLE IF EXISTS kv;
DROP TABLE IF EXISTS messages;
DROP TABLE IF EXISTS conversation_members;
DROP TABLE IF EXISTS conversations;
DROP TABLE IF EXISTS friends;
DROP TABLE IF EXISTS users;
