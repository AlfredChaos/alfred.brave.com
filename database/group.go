package database

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// GroupMaxMembers 群成员上限（§5 约束①：拉人事务内 COUNT 校验）。
const GroupMaxMembers = 500

// Group 群（表 groups）。gid 即会话 conv_id（雪花 ID 字符串）。
type Group struct {
	GID            string     `json:"gid"`
	Name           string     `json:"name"`
	Announcement   string     `json:"announcement"`
	AnnouncementAt *time.Time `json:"announcement_at,omitempty"`
	OwnerUID       string     `json:"owner_uid"`
	PinnedMsgID    *string    `json:"pinned_msg_id,omitempty"`
	Status         string     `json:"status"`
	MemberCount    int        `json:"member_count"`
	CreatedAt      time.Time  `json:"created_at"`
}

// GroupMember 群成员。left_at 非空 = 已退/被踢（历史可见性以 joined_at/left_at 界定）。
type GroupMember struct {
	GID      string     `json:"gid"`
	UID      string     `json:"uid"`
	Role     string     `json:"role"` // owner|member
	JoinedAt time.Time  `json:"joined_at"`
	LeftAt   *time.Time `json:"left_at,omitempty"`
}

// 群状态与角色常量（§5）。
const (
	GroupStatusActive    = "active"
	GroupStatusDismissed = "dismissed"
	GroupRoleOwner       = "owner"
	GroupRoleMember      = "member"
)

// 群事件类型（system_event content.event，D09）。
const (
	EventGroupCreate     = "group_create"
	EventMemberJoin      = "member_join"
	EventMemberKick      = "member_kick"
	EventMemberQuit      = "member_quit"
	EventGroupRename     = "group_rename"
	EventAnnouncementSet = "announcement_set"
	EventPinSet          = "pin_set"
	EventPinUnset        = "pin_unset"
	EventGroupDismiss    = "group_dismiss"
)

// 管理操作的授权/约束错误（API 层映射 4xx）。
var (
	ErrNotOwner       = errors.New("group: operator is not owner")
	ErrNotMember      = errors.New("group: operator is not a member")
	ErrGroupFull      = errors.New("group: member limit exceeded")
	ErrGroupDismissed = errors.New("group: group dismissed")
	ErrBadTarget      = errors.New("group: invalid target member")
	ErrOwnerCantQuit  = errors.New("group: owner must dismiss instead of quit")
)

// GroupIDFunc gid 生成器注入（雪花；测试可替换）。
type GroupIDFunc func() (string, error)

type GroupStore struct {
	store *Store
	kv    *KvStore
}

func NewGroupStore(s *Store) *GroupStore {
	return &GroupStore{store: s, kv: NewKvStore(s)}
}

// insertSystemEvent 事务内写群事件消息（D09：与群变更同事务，共享 seq 流）。
// 返回 msg_id——调用方事务提交后 produce chat.msg（带预写 msg_id）交 persist 幂等扇出。
func (gs *GroupStore) insertSystemEvent(ctx context.Context, tx pgx.Tx, gid, actor string, content []byte) (string, error) {
	seq, err := gs.kv.NextSeqTx(ctx, tx, gid)
	if err != nil {
		return "", err
	}
	msgID := uuid.NewString()
	if _, err = tx.Exec(ctx,
		`INSERT INTO messages (msg_id, conv_id, seq, from_uid, type, content)
		 VALUES ($1, $2, $3, $4, 'system_event', $5::jsonb)`,
		msgID, gid, seq, actor, content); err != nil {
		return "", err
	}
	if _, err = tx.Exec(ctx,
		`UPDATE conversations SET last_seq = $2 WHERE conv_id = $1 AND last_seq < $2`, gid, seq); err != nil {
		return "", err
	}
	return msgID, nil
}

// CreateGroup 建群事务：groups + 成员行 + 会话表同步 + 建群事件消息。
func (gs *GroupStore) CreateGroup(ctx context.Context, idGen GroupIDFunc, ownerUID, name string, memberUIDs []string) (gid, eventMsgID string, err error) {
	if ownerUID == "" || name == "" {
		return "", "", ErrBadTarget
	}
	gid, err = idGen()
	if err != nil {
		return "", "", err
	}

	tx, err := gs.store.pool.Begin(ctx)
	if err != nil {
		return "", "", err
	}
	defer tx.Rollback(ctx)

	if _, err = tx.Exec(ctx,
		`INSERT INTO groups (gid, name, owner_uid, member_count) VALUES ($1, $2, $3, $4)`,
		gid, name, ownerUID, len(memberUIDs)+1); err != nil {
		return "", "", err
	}
	all := make([]string, 0, len(memberUIDs)+1)
	all = append(all, ownerUID)
	if _, err = tx.Exec(ctx,
		`INSERT INTO group_members (gid, uid, role) VALUES ($1, $2, 'owner')`, gid, ownerUID); err != nil {
		return "", "", err
	}
	for _, uid := range memberUIDs {
		if uid == ownerUID {
			continue
		}
		all = append(all, uid)
		if _, err = tx.Exec(ctx,
			`INSERT INTO group_members (gid, uid, role) VALUES ($1, $2, 'member')`, gid, uid); err != nil {
			return "", "", err
		}
	}

	// 会话表同步（conv_id=gid；members JSONB 全量，管理操作维护）
	membersJSON, _ := json.Marshal(all)
	if _, err = tx.Exec(ctx,
		`INSERT INTO conversations (conv_id, type, members) VALUES ($1, 'group', $2::jsonb)`,
		gid, membersJSON); err != nil {
		return "", "", err
	}
	for _, uid := range all {
		if _, err = tx.Exec(ctx,
			`INSERT INTO conversation_members (conv_id, uid) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
			gid, uid); err != nil {
			return "", "", err
		}
	}

	content, _ := json.Marshal(map[string]interface{}{"event": EventGroupCreate, "actor": ownerUID, "name": name})
	eventMsgID, err = gs.insertSystemEvent(ctx, tx, gid, ownerUID, content)
	if err != nil {
		return "", "", err
	}
	return gid, eventMsgID, tx.Commit(ctx)
}

// GetGroup 群详情。
func (gs *GroupStore) GetGroup(ctx context.Context, gid string) (*Group, error) {
	g := &Group{}
	err := gs.store.pool.QueryRow(ctx,
		`SELECT gid, name, announcement, announcement_at, owner_uid, pinned_msg_id, status, member_count, created_at
		 FROM groups WHERE gid = $1`, gid).
		Scan(&g.GID, &g.Name, &g.Announcement, &g.AnnouncementAt, &g.OwnerUID, &g.PinnedMsgID, &g.Status, &g.MemberCount, &g.CreatedAt)
	if err != nil {
		return nil, wrapNoRows(err)
	}
	return g, nil
}

// GetMember 成员行（含已退）。
func (gs *GroupStore) GetMember(ctx context.Context, gid, uid string) (*GroupMember, error) {
	m := &GroupMember{}
	err := gs.store.pool.QueryRow(ctx,
		`SELECT gid, uid, role, joined_at, left_at FROM group_members WHERE gid = $1 AND uid = $2`,
		gid, uid).Scan(&m.GID, &m.UID, &m.Role, &m.JoinedAt, &m.LeftAt)
	if err != nil {
		return nil, wrapNoRows(err)
	}
	return m, nil
}

// ActiveMembers 当前有效成员（left_at IS NULL），扇出与校验用。
func (gs *GroupStore) ActiveMembers(ctx context.Context, gid string) ([]GroupMember, error) {
	rows, err := gs.store.pool.Query(ctx,
		`SELECT gid, uid, role, joined_at, left_at FROM group_members
		 WHERE gid = $1 AND left_at IS NULL ORDER BY joined_at`, gid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	members := make([]GroupMember, 0)
	for rows.Next() {
		var m GroupMember
		if err := rows.Scan(&m.GID, &m.UID, &m.Role, &m.JoinedAt, &m.LeftAt); err != nil {
			return nil, err
		}
		members = append(members, m)
	}
	return members, rows.Err()
}

// ListGroupsByUser 用户加入且未退出的群。
func (gs *GroupStore) ListGroupsByUser(ctx context.Context, uid string) ([]Group, error) {
	rows, err := gs.store.pool.Query(ctx,
		`SELECT g.gid, g.name, g.announcement, g.announcement_at, g.owner_uid, g.pinned_msg_id, g.status, g.member_count, g.created_at
		 FROM groups g JOIN group_members m ON m.gid = g.gid
		 WHERE m.uid = $1 AND m.left_at IS NULL AND g.status = 'active'
		 ORDER BY g.created_at DESC LIMIT 200`, uid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	groups := make([]Group, 0)
	for rows.Next() {
		var g Group
		if err := rows.Scan(&g.GID, &g.Name, &g.Announcement, &g.AnnouncementAt, &g.OwnerUID, &g.PinnedMsgID, &g.Status, &g.MemberCount, &g.CreatedAt); err != nil {
			return nil, err
		}
		groups = append(groups, g)
	}
	return groups, rows.Err()
}

// mutateAsOwner owner-only 管理操作公共骨架：群存活 + 操作者是 owner 校验后执行变更与事件写入。
func (gs *GroupStore) mutateAsOwner(ctx context.Context, gid, operator string, event string,
	change func(tx pgx.Tx) error, eventExtra map[string]interface{}) (string, error) {
	tx, err := gs.store.pool.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)

	var status, owner string
	if err = tx.QueryRow(ctx,
		`SELECT status, owner_uid FROM groups WHERE gid = $1 FOR UPDATE`, gid).Scan(&status, &owner); err != nil {
		return "", wrapNoRows(err)
	}
	if status == GroupStatusDismissed {
		return "", ErrGroupDismissed
	}
	if owner != operator {
		return "", ErrNotOwner
	}
	if change != nil {
		if err = change(tx); err != nil {
			return "", err
		}
	}
	payload := map[string]interface{}{"event": event, "actor": operator}
	for k, v := range eventExtra {
		payload[k] = v
	}
	content, _ := json.Marshal(payload)
	msgID, err := gs.insertSystemEvent(ctx, tx, gid, operator, content)
	if err != nil {
		return "", err
	}
	return msgID, tx.Commit(ctx)
}

// AddMembers 拉人（owner-only；事务内 COUNT<500 校验；重复成员幂等跳过）。
func (gs *GroupStore) AddMembers(ctx context.Context, gid, operator string, uids []string) (string, error) {
	if len(uids) == 0 {
		return "", ErrBadTarget
	}
	return gs.mutateAsOwner(ctx, gid, operator, EventMemberJoin, func(tx pgx.Tx) error {
		var active int
		if err := tx.QueryRow(ctx,
			`SELECT count(*) FROM group_members WHERE gid = $1 AND left_at IS NULL`, gid).Scan(&active); err != nil {
			return err
		}
		// 过滤已在群的 uid（幂等）
		newUIDs := make([]string, 0, len(uids))
		for _, uid := range uids {
			var exists bool
			if err := tx.QueryRow(ctx,
				`SELECT EXISTS(SELECT 1 FROM group_members WHERE gid=$1 AND uid=$2 AND left_at IS NULL)`,
				gid, uid).Scan(&exists); err != nil {
				return err
			}
			if !exists {
				newUIDs = append(newUIDs, uid)
			}
		}
		if active+len(newUIDs) > GroupMaxMembers {
			return ErrGroupFull
		}
		for _, uid := range newUIDs {
			if _, err := tx.Exec(ctx,
				`INSERT INTO group_members (gid, uid, role) VALUES ($1, $2, 'member') ON CONFLICT (gid, uid)
				 DO UPDATE SET left_at = NULL, joined_at = now()`, gid, uid); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx,
				`INSERT INTO conversation_members (conv_id, uid) VALUES ($1, $2) ON CONFLICT DO NOTHING`, gid, uid); err != nil {
				return err
			}
		}
		if _, err := tx.Exec(ctx,
			`UPDATE groups SET member_count = (SELECT count(*) FROM group_members WHERE gid=$1 AND left_at IS NULL) WHERE gid=$1`, gid); err != nil {
			return err
		}
		return nil
	}, map[string]interface{}{"members": uids})
}

// RemoveMember 踢人（owner-only；不可踢自己/owner——即群主）。
func (gs *GroupStore) RemoveMember(ctx context.Context, gid, operator, target string) (string, error) {
	g, err := gs.GetGroup(ctx, gid)
	if err != nil {
		return "", err
	}
	if g.OwnerUID == target {
		return "", ErrBadTarget // 不可踢群主（含自己）
	}
	return gs.mutateAsOwner(ctx, gid, operator, EventMemberKick, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx,
			`UPDATE group_members SET left_at = now() WHERE gid=$1 AND uid=$2 AND left_at IS NULL`, gid, target)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return ErrBadTarget
		}
		if _, err := tx.Exec(ctx,
			`UPDATE groups SET member_count = (SELECT count(*) FROM group_members WHERE gid=$1 AND left_at IS NULL) WHERE gid=$1`, gid); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx,
			`DELETE FROM conversation_members WHERE conv_id=$1 AND uid=$2`, gid, target); err != nil {
			return err
		}
		return nil
	}, map[string]interface{}{"target": target})
}

// Quit 退群（member-only；owner 拒绝——只能解散，§5 权限矩阵）。
func (gs *GroupStore) Quit(ctx context.Context, gid, uid string) (string, error) {
	tx, err := gs.store.pool.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)

	var status, role string
	err = tx.QueryRow(ctx,
		`SELECT g.status, m.role FROM groups g JOIN group_members m ON m.gid = g.gid
		 WHERE g.gid = $1 AND m.uid = $2 AND m.left_at IS NULL FOR UPDATE OF m`, gid, uid).Scan(&status, &role)
	if err != nil {
		return "", wrapNoRows(err)
	}
	if status == GroupStatusDismissed {
		return "", ErrGroupDismissed
	}
	if role == GroupRoleOwner {
		return "", ErrOwnerCantQuit
	}
	if _, err = tx.Exec(ctx,
		`UPDATE group_members SET left_at = now() WHERE gid=$1 AND uid=$2`, gid, uid); err != nil {
		return "", err
	}
	if _, err = tx.Exec(ctx,
		`UPDATE groups SET member_count = (SELECT count(*) FROM group_members WHERE gid=$1 AND left_at IS NULL) WHERE gid=$1`, gid); err != nil {
		return "", err
	}
	if _, err = tx.Exec(ctx,
		`DELETE FROM conversation_members WHERE conv_id=$1 AND uid=$2`, gid, uid); err != nil {
		return "", err
	}
	content, _ := json.Marshal(map[string]interface{}{"event": EventMemberQuit, "actor": uid})
	msgID, err := gs.insertSystemEvent(ctx, tx, gid, uid, content)
	if err != nil {
		return "", err
	}
	return msgID, tx.Commit(ctx)
}

// Rename 改名（owner-only）。
func (gs *GroupStore) Rename(ctx context.Context, gid, operator, name string) (string, error) {
	if name == "" {
		return "", ErrBadTarget
	}
	return gs.mutateAsOwner(ctx, gid, operator, EventGroupRename, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE groups SET name = $2 WHERE gid = $1`, gid, name)
		return err
	}, map[string]interface{}{"name": name})
}

// SetAnnouncement 公告（owner-only，更新 announcement_at）。
func (gs *GroupStore) SetAnnouncement(ctx context.Context, gid, operator, text string) (string, error) {
	return gs.mutateAsOwner(ctx, gid, operator, EventAnnouncementSet, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE groups SET announcement = $2, announcement_at = now() WHERE gid = $1`, gid, text)
		return err
	}, map[string]interface{}{"text": text})
}

// SetPin 置顶（owner-only；msgID 空串=取消置顶）。
func (gs *GroupStore) SetPin(ctx context.Context, gid, operator, msgID string) (string, error) {
	event := EventPinSet
	if msgID == "" {
		event = EventPinUnset
	}
	return gs.mutateAsOwner(ctx, gid, operator, event, func(tx pgx.Tx) error {
		if msgID == "" {
			_, err := tx.Exec(ctx, `UPDATE groups SET pinned_msg_id = NULL WHERE gid = $1`, gid)
			return err
		}
		_, err := tx.Exec(ctx, `UPDATE groups SET pinned_msg_id = $2 WHERE gid = $1`, gid, msgID)
		return err
	}, map[string]interface{}{"msg_id": msgID})
}

// Dismiss 解散（owner-only）：状态 tombstone；persist 拒收该 gid 一切后续消息。
// 解散事件是最后一条消息。
func (gs *GroupStore) Dismiss(ctx context.Context, gid, operator string) (string, error) {
	return gs.mutateAsOwner(ctx, gid, operator, EventGroupDismiss, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE groups SET status = 'dismissed' WHERE gid = $1`, gid)
		return err
	}, nil)
}
