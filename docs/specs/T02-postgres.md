# T02 · PostgreSQL 迁移（goose + pgx v5 + CRUD 改写）

## 1. 目标与范围

**做**：
1. goose v3（postgres 方言）迁移脚本：users / friends / conversations / conversation_members / messages / kv 六表（§7 DDL，含索引与约束）。
2. pgx v5 连接层：`database.Store` 封装 pgxpool，构造函数注入，替换 jinzhu/gorm（含 conf.Config 的 DB 中间件改写）。
3. 现有用户/好友 CRUD 改写为 pgx 手写 SQL，通过**原行为等价**的集成测试。
4. kv 表数据访问层（Upsert/Get/Delete/条件删除/前缀扫描/seq 原子自增）——T04（online）与 T05（seq）共用。
5. 依赖治理：go mod tidy；satori/go.uuid → google/uuid；jinzhu/gorm、MySQL driver 移除。

**不做**：conversations/messages 业务写入路径（T05）、groups/feed 表（T13/T09）、PG 流复制（T18 生产脚本）。

## 2. 接口契约

### DDL（对齐 §7，snake_case 列名）

- `users(uid VARCHAR(36) PK, user_name UNIQUE, email UNIQUE, password_hash BYTEA, profile, avatar BYTEA, login_at, created_at, updated_at)`
- `friends(owner_uid, friend_uid, status SMALLINT DEFAULT 1, created_at) PK(owner_uid, friend_uid)`，索引 `(friend_uid)`（D07：双边存两行）
- `conversations(conv_id VARCHAR(64) PK, type CHECK(single|group), single_key VARCHAR(141) UNIQUE NULL, members JSONB, last_seq BIGINT DEFAULT 0, created_at)`
  - single_key = `min(uidA,uidB)+":"+max(uidA,uidB)`，唯一约束防 A→B / B→A 重复建会话（群聊为 NULL 不受约束）
- `conversation_members(conv_id, uid, last_read_seq BIGINT DEFAULT 0, muted BOOL DEFAULT false, created_at) PK(conv_id, uid)`
- `messages(msg_id VARCHAR(36) PK, conv_id, seq BIGINT, from_uid, type CHECK(single|group|system_event), content JSONB, created_at)`，**UNIQUE(conv_id, seq)**（D08/D16 幂等防线），索引 `(conv_id, seq DESC)`
- `kv(k TEXT PK, v JSONB, version BIGINT DEFAULT 1, updated_at)`，前缀扫描索引 `k text_pattern_ops`

### 存储接口（构造注入）

```go
database.NewStore(ctx, dsn, maxConns) (*Store, error)   // pgxpool + 重试
database.NewUserStore(*Store).Create/Get/GetByUserName/GetByEmail/List/Delete
database.NewFriendStore(*Store).Create(双边)/ListByOwner/GetFriendship/Delete(双边)
database.NewKvStore(*Store).Put/Get/Delete/DeleteIfMatch(scan 防漂移)/ScanPrefix/NextSeq(事务原子自增)
```

### 配置（etc/*.yaml）

```yaml
database_driver: "postgres"
postgres: { user, password, server, port, database, conns, conns_idle }
```

## 3. 架构符合性声明

- **D04**：PG15 + pgx v5 手写 SQL + goose v3 postgres 方言；JSONB/事务内自增/COPY 能力就位。
- **D05**：kv 表落地，etcd 后续退回纯服务表（T04 完成 users/{uid} 迁移后移除）。
- **D07**：friends 双边两行 + 复合主键，无图数据库。
- **D16**：messages 不分区，UNIQUE(conv_id, seq) 全局有效；conv_id VARCHAR(64) 兼容雪花 ID 字符串。
- 中间件常量 `MiddlewareMysql` → `MiddlewareDatabase`（DB 层被替换后的诚实命名，非重构）。

## 4. 测试计划

- 单元（无依赖）：DSN 构造、singleKey 规范化（min:max 与参数顺序无关）。
- 集成（`-tags=integration`，连本地 docker PG）：goose up → 用户 CRUD 等价（create/get 三途径/list/唯一冲突）、好友双边写/查/删、kv Put/Get/条件删/前缀扫/NextSeq 并发自增不重号。

## 5. 验收标准

```bash
gofmt -l . && go vet ./... && go build ./... && go test ./...          # 全绿
docker run -d --name brave-pg-t02 -e POSTGRES_USER=brave -e POSTGRES_PASSWORD=brave -e POSTGRES_DB=brave -p 55432:5432 postgres:15
BRAVE_PG_DSN='postgres://brave:brave@127.0.0.1:55432/brave?sslmode=disable' \
  go test -tags=integration ./database/ -v    # 全绿
```

## 6. 验收记录（实测粘贴）

- `gofmt -l .` → 空；`go vet ./...` → 通过；`go build ./...` → 通过
- `go test ./...` → ok alfred.brave.com/database / joker/exchange / server/api，0 失败
- 集成（docker postgres:15 @127.0.0.1:55432，goose 自动迁移后）：
  ```
  --- PASS: TestSingleKey (0.00s)
  --- PASS: TestUserCRUDEquivalence (0.08s)
  --- PASS: TestFriendDoubleRow (0.07s)
  --- PASS: TestKvStore (0.04s)
  --- PASS: TestKvNextSeq (0.05s)   // 串行严格递增 + 50 并发无重号
  --- PASS: TestConversationSingleKeyUnique (0.02s)
  ok   alfred.brave.com/database	0.136s
  ```
- 修复记录：NextSeq 首版 INSERT 初始 next:0 导致首条消息 seq=0（红灯发现），改初始 1 后通过——先红后绿成立。

## 7. Review 记录

- DDL 与 §7 逐表对齐 ✅；UNIQUE(conv_id,seq) 已建并由 T05 幂等测试覆盖（本任务建约束）✅
- 构造注入，无新增包级可变全局 ✅；错误显式处理（pgx.ErrNoRows → database.ErrNotFound）✅
- 删除项：MySQL 迁移脚本、docker/mysql 初始化 SQL（git 可回溯）✅
