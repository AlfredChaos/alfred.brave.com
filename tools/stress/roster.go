// roster.go —— 压测账号账簿生成器（stress-plan.md §4.5）。
//
// 背景：65–75 万连接需要同等数量预注册账号。走 API 注册不可行——bcrypt cost10
// 每次 ~50–100ms CPU，80 万次注册要 5–11 小时，且登录验证同样吃 bcrypt，
// 建连吞吐会被卡死在 ~50–100/s。
//
// 方案：直写 PG 批量造号 + bcrypt cost4 哈希（登录验证 ~1ms，网关登录吞吐回到
// 千级 QPS）。全部账号共用同一明文密码（Stress123）→ 只需预计算一个 cost4 哈希。
// 诚实边界：压测专用账号池，绕过注册 API（注册路径已有 e2e 覆盖）；cost4 仅用于
// 压测账号，生产注册仍走 cost10（server/api/register.go）。
//
// 用法：
//
//	go run ./tools/stress -mode roster -dsn postgres://brave:brave@127.0.0.1:55432/brave?sslmode=disable \
//	  -seed n1 -users 300000 -out results/n1-roster
//
// 产出：out/roster.csv（name,uid,email），幂等可重跑（ON CONFLICT DO NOTHING）。
package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"golang.org/x/crypto/bcrypt"
)

const rosterPassword = "Stress123"

// rosterBcryptCost 4：压测账号专用低代价哈希（见文件头说明）。
const rosterBcryptCost = 4

func runRoster() error {
	if *dsn == "" {
		return fmt.Errorf("-dsn is required for roster mode")
	}
	if *users <= 0 {
		return fmt.Errorf("-users must be positive")
	}
	out := *outDir
	if out == "" {
		out = fmt.Sprintf("roster-%s", *seed)
	}
	if err := os.MkdirAll(out, 0o755); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	conn, err := pgx.Connect(ctx, *dsn)
	if err != nil {
		return fmt.Errorf("connect pg: %w", err)
	}
	defer conn.Close(context.Background())

	// 预计算一个 cost4 哈希：所有压测账号共用（登录验证对"哈希+明文"成立即可）
	hash, err := bcrypt.GenerateFromPassword([]byte(rosterPassword), rosterBcryptCost)
	if err != nil {
		return fmt.Errorf("bcrypt: %w", err)
	}
	if bcrypt.CompareHashAndPassword(hash, []byte(rosterPassword)) != nil {
		return fmt.Errorf("hash self-check failed")
	}

	start := time.Now()
	inserted, existing, err := seedUsers(ctx, conn, hash)
	if err != nil {
		return err
	}

	// 账簿 CSV：name,uid,email（uid 与库中一致——已存在行读回）
	csvPath := filepath.Join(out, "roster.csv")
	if err := writeRosterCSV(ctx, conn, csvPath); err != nil {
		return err
	}

	fmt.Printf("roster done in %s: inserted=%d existing=%d total=%d -> %s\n",
		time.Since(start).Round(time.Millisecond), inserted, existing, inserted+existing, csvPath)
	return nil
}

// seedUsers 批量造号：temp 表 COPY + INSERT ... ON CONFLICT DO NOTHING（幂等）。
func seedUsers(ctx context.Context, conn *pgx.Conn, hash []byte) (inserted, existing int, err error) {
	if _, err = conn.Exec(ctx, `CREATE TEMP TABLE roster_stage (uid text, user_name text, email text, hash bytea, ts timestamptz)`); err != nil {
		return 0, 0, err
	}

	// 分批 COPY（10 万/批，控制 temp 表与内存）
	const copyBatch = 100000
	total := 0
	for base := 0; base < *users; base += copyBatch {
		end := base + copyBatch
		if end > *users {
			end = *users
		}
		rows := make([][]any, 0, end-base)
		for i := base; i < end; i++ {
			name := fmt.Sprintf("%s-%07d", *seed, i)
			rows = append(rows, []any{uuid.NewString(), name, name + "@stress.local", hash, time.Now()})
		}
		if _, err = conn.CopyFrom(ctx, pgx.Identifier{"roster_stage"},
			[]string{"uid", "user_name", "email", "hash", "ts"}, pgx.CopyFromRows(rows)); err != nil {
			return 0, 0, fmt.Errorf("copy batch %d: %w", base, err)
		}
		total += len(rows)
	}

	// 幂等落库 + 回填已存在账号的 uid（账簿 uid 与库一致）
	if _, err = conn.Exec(ctx, `
		INSERT INTO users (uid, user_name, email, password_hash, profile, login_at, created_at, updated_at)
		SELECT uid, user_name, email, hash, '', ts, ts, ts FROM roster_stage
		ON CONFLICT (user_name) DO NOTHING`); err != nil {
		return 0, 0, fmt.Errorf("insert: %w", err)
	}
	if err = conn.QueryRow(ctx, `SELECT count(*) FROM roster_stage s
		JOIN users u ON u.user_name = s.user_name AND u.uid = s.uid`).Scan(&inserted); err != nil {
		return 0, 0, err
	}
	existing = total - inserted
	return inserted, existing, nil
}

// writeRosterCSV 按 seed 前缀把库中账号（含本次与历史批次）导出为账簿。
func writeRosterCSV(ctx context.Context, conn *pgx.Conn, path string) error {
	rows, err := conn.Query(ctx, `SELECT user_name, uid, email FROM users
		WHERE user_name LIKE $1 || '-%' ORDER BY user_name`, *seed)
	if err != nil {
		return err
	}
	defer rows.Close()

	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	if err = w.Write([]string{"user_name", "uid", "email"}); err != nil {
		return err
	}
	n := 0
	for rows.Next() {
		var name, uid, email string
		if err = rows.Scan(&name, &uid, &email); err != nil {
			return err
		}
		if err = w.Write([]string{name, uid, email}); err != nil {
			return err
		}
		n++
	}
	fmt.Printf("roster csv rows: %d\n", n)
	return rows.Err()
}
