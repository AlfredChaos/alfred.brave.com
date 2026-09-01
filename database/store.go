package database

import (
	"context"
	"errors"
	"time"

	"alfred.brave.com/event"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var log = event.Log

// ErrNotFound 统一“记录不存在”语义（pgx.ErrNoRows 的别名），调用方判等无需依赖驱动包。
var ErrNotFound = errors.New("record not found")

// Store pgx 连接池封装：所有数据访问器的注入源（构造注入，替代原 gorm 全局 provider）。
// 并发安全：pgxpool 本身并发安全，Store 无自有可变状态。
type Store struct {
	pool *pgxpool.Pool
}

// NewStore 建立 PG 连接池，带重试（compose 环境里 PG 可能晚于业务进程就绪）。
func NewStore(ctx context.Context, dsn string, maxConns int32) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		log.Errorf("parse pg dsn failed: %v", err)
		return nil, err
	}
	if maxConns > 0 {
		cfg.MaxConns = maxConns
	}
	cfg.MaxConnLifetime = time.Hour

	var pool *pgxpool.Pool
	for attempt := 1; attempt <= 12; attempt++ {
		pool, err = pgxpool.NewWithConfig(ctx, cfg)
		if err == nil {
			pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			err = pool.Ping(pingCtx)
			cancel()
			if err == nil {
				break
			}
			pool.Close()
		}
		log.Warnf("connect postgres attempt %d failed: %v", attempt, err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(5 * time.Second):
		}
	}
	if err != nil {
		log.Errorf("connect postgres finally failed: %v", err)
		return nil, err
	}
	return &Store{pool: pool}, nil
}

// Pool 暴露原生池，供需要事务/COPY 的高级路径使用。
func (s *Store) Pool() *pgxpool.Pool {
	return s.pool
}

func (s *Store) Close() {
	if s.pool != nil {
		s.pool.Close()
	}
}

// pgxRow 统一 QueryRow 与 Rows 的 Scan 接口（模型扫描辅助用）。
type pgxRow interface {
	Scan(dest ...interface{}) error
}

// wrapNoRows 把驱动的 ErrNoRows 归一为 database.ErrNotFound。
func wrapNoRows(err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	return err
}
