package database

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// User 用户模型（表 users）。字段与 json tag 对齐原 gorm 版本，保证 API 响应结构不变。
type User struct {
	UID       string    `json:"uid"`
	UserName  string    `json:"user_name"`
	Email     string    `json:"email"`
	Profile   string    `json:"profile"`
	Avatar    []byte    `json:"avatar"`
	Password  []byte    `json:"password"` // bcrypt 散列（存储列 password_hash）
	LoginAt   time.Time `json:"login_at,omitempty"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// UserFilters 用户列表过滤条件（与原版语义一致：单条件模糊匹配 / IN 列表）。
type UserFilters struct {
	UserName     *string
	UserNameList []string
	Email        *string
	EmailList    []string
}

type UserStore struct {
	store *Store
}

func NewUserStore(s *Store) *UserStore {
	return &UserStore{store: s}
}

// Create 创建用户；UID 服务端生成（google/uuid，替换已归档的 satori）。
func (us *UserStore) Create(ctx context.Context, u *User) error {
	if u.UID == "" {
		u.UID = uuid.NewString()
	}
	now := time.Now()
	u.CreatedAt, u.UpdatedAt = now, now
	_, err := us.store.pool.Exec(ctx,
		`INSERT INTO users (uid, user_name, email, password_hash, profile, avatar, login_at, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		u.UID, u.UserName, u.Email, u.Password, u.Profile, u.Avatar, u.LoginAt, u.CreatedAt, u.UpdatedAt)
	return err
}

const userColumns = `uid, user_name, email, password_hash, profile, avatar, login_at, created_at, updated_at`

func scanUser(row pgxRow, u *User) error {
	return row.Scan(&u.UID, &u.UserName, &u.Email, &u.Password, &u.Profile, &u.Avatar,
		&u.LoginAt, &u.CreatedAt, &u.UpdatedAt)
}

func (us *UserStore) Get(ctx context.Context, uid string) (*User, error) {
	u := &User{}
	err := scanUser(us.store.pool.QueryRow(ctx,
		`SELECT `+userColumns+` FROM users WHERE uid = $1`, uid), u)
	if err != nil {
		return nil, wrapNoRows(err)
	}
	return u, nil
}

func (us *UserStore) GetByUserName(ctx context.Context, name string) (*User, error) {
	u := &User{}
	err := scanUser(us.store.pool.QueryRow(ctx,
		`SELECT `+userColumns+` FROM users WHERE user_name = $1`, name), u)
	if err != nil {
		return nil, wrapNoRows(err)
	}
	return u, nil
}

func (us *UserStore) GetByEmail(ctx context.Context, email string) (*User, error) {
	u := &User{}
	err := scanUser(us.store.pool.QueryRow(ctx,
		`SELECT `+userColumns+` FROM users WHERE email = $1`, email), u)
	if err != nil {
		return nil, wrapNoRows(err)
	}
	return u, nil
}

// List 条件列表；无条件时返回全部（等价原 gorm List 行为，且修复原版切片按值传参结果被丢弃的 bug）。
func (us *UserStore) List(ctx context.Context, filters *UserFilters) ([]User, error) {
	users := make([]User, 0)
	sql := `SELECT ` + userColumns + ` FROM users`
	var args []interface{}

	if filters != nil {
		switch {
		case filters.UserName != nil:
			sql += ` WHERE user_name LIKE '%' || $1 || '%'`
			args = append(args, *filters.UserName)
		case filters.Email != nil:
			sql += ` WHERE email LIKE '%' || $1 || '%'`
			args = append(args, *filters.Email)
		case len(filters.EmailList) != 0:
			sql += ` WHERE email = ANY($1)`
			args = append(args, filters.EmailList)
		case len(filters.UserNameList) != 0:
			sql += ` WHERE user_name = ANY($1)`
			args = append(args, filters.UserNameList)
		}
	}
	sql += ` ORDER BY created_at DESC LIMIT 500`

	rows, err := us.store.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var u User
		if err := scanUser(rows, &u); err != nil {
			return nil, err
		}
		users = append(users, u)
	}
	return users, rows.Err()
}

// UpdateLoginAt 登录成功后刷新 login_at/updated_at（原版 Update 全字段更新收窄为实际用途）。
func (us *UserStore) UpdateLoginAt(ctx context.Context, uid string) error {
	_, err := us.store.pool.Exec(ctx,
		`UPDATE users SET login_at = now(), updated_at = now() WHERE uid = $1`, uid)
	return err
}
