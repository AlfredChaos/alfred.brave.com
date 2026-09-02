-- +goose Up
-- 群聊域两表（§5）。gid 为雪花 ID 字符串（D16），同时充当 conversations.conv_id。
-- 群事件即消息（D09）：管理操作在事务内写 type=system_event 消息进同一 seq 流。
CREATE TABLE groups (
    gid             VARCHAR(64)  PRIMARY KEY,
    name            VARCHAR(64)  NOT NULL,
    announcement    VARCHAR(500) NOT NULL DEFAULT '',
    announcement_at TIMESTAMPTZ,
    owner_uid       VARCHAR(36)  NOT NULL,
    pinned_msg_id   VARCHAR(36),
    status          VARCHAR(16)  NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'dismissed')),
    member_count    INT          NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE group_members (
    gid      VARCHAR(64)  NOT NULL,
    uid      VARCHAR(36)  NOT NULL,
    role     VARCHAR(16)  NOT NULL DEFAULT 'member' CHECK (role IN ('owner', 'member')),
    joined_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    left_at  TIMESTAMPTZ,
    PRIMARY KEY (gid, uid)
);
-- 用户群列表
CREATE INDEX idx_group_members_uid ON group_members (uid);
-- 拉人上限校验的计数路径
CREATE INDEX idx_group_members_gid_active ON group_members (gid) WHERE left_at IS NULL;

-- +goose Down
DROP TABLE IF EXISTS group_members;
DROP TABLE IF EXISTS groups;
