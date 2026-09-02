# docs/specs · 差距盘点与任务规格索引

> 依据 `docs/dev-task-prompt.md` §10 启动指令产出：先盘点当前代码与 `target-architecture.html` v4 的差距，再按 T01-T19 逐任务四阶段（SDD→TDD→验收→Review）推进。每任务一份 `T<NN>-<slug>.md` 规格。

## 1. 现状盘点（2026-09-01，基线 commit 8708c5c）

### 已有框架（沿用，不重写）

| 模块 | 现状 | 目标架构中的角色 |
|---|---|---|
| `cmd/brave.go` + `commands/` | urfave/cli 子命令派发（start/joker/cloudware/migration） | 单二进制多服务（start/joker/feed/persist/deliver/fanout/push/ghost）扩展点 |
| `conf/` + `event/` | viper + fsnotify 配置中间件（Mysql/Etcd middleware）、包级 logger | 沿用；新增 PG/Kafka 配置项 |
| `internal/etcd` | Register（60s lease+KeepAlive）/ Discovery（Watch+GetServices）；UserFactory 存登录态 | 服务注册发现沿用；**UserFactory（users/{uid}）废弃→PG kv online:{uid}** |
| `joker/exchange` | Manager(sync.Map)+Client(Read/WritePump)，WS /ws/:id | 哑管道 CS：收消息→produce chat.msg；gRPC :37012 收投递；online kv 唯一写者 |
| `server/` | REST（register/login/get_user/index），登录随机选 CS（getServiceByRandom），调 joker HTTP /login 写 etcd 登录态 | 网关化：token 鉴权、/v1/*、下发 ws_addr、会话/历史/群管理/朋友 API、/v1/feed/* 路由 |
| `database/` | jinzhu/gorm v1 + MySQL，User/Friend CRUD | **改写 pgx v5 + PG**；新增 messages/kv/groups/feed 全套 |
| `docker/` + compose | mysql + 3×work(etcd+joker) + server | 重构为 deploy/local（PG/etcd/kafka/单实例业务×2 joker） |
| `internal/{abort,i18n,http_client,mutex}` | 统一错误响应、i18n、出站 HTTP | 沿用（网关→feed-api 转发用 http_client） |

### 差距清单（按目标架构章节）

| # | 目标（章节/ADR） | 当前状态 | 落地任务 |
|---|---|---|---|
| 1 | 心跳 D14：客户端 30s 上报 + 6 分钟超时清理 | `client.go:55 ticker 1ns` bug；无心跳处理 | T01 |
| 2 | `rand.Seed` 并发不安全（AGENTS §5.6） | `server/api/login.go` 每次调用 rand.Seed | T01 |
| 3 | 参数校验（AGENTS §5.6） | VerifyUserName/Email/Password 空函数 | T01 |
| 4 | PG 15 + pgx v5 + goose(postgres)（§7/D04） | MySQL + jinzhu/gorm v1 | T02 |
| 5 | 网关 token 鉴权 + login 返回 {token, ws_addr}（§2/§3 0a-0d） | login 返回 login_host；调 joker HTTP 写 etcd 登录态 | T03 |
| 6 | cloudware 删除（§1 决策表） | 3 空壳端点无调用方 | T03 |
| 7 | online:{uid} kv（Joker upsert/del，D06/§4） | etcd users/{uid} 无 TTL 登录态 | T04 |
| 8 | GHOST 对账任务 60s（§4） | 无 | T04 |
| 9 | Kafka chat.msg 12P + persist worker（§8/D02/D18/D19/D20） | 无 Kafka 代码 | T05 |
| 10 | kv seq:{conv_id} 事务自增 + messages UNIQUE(conv_id,seq)（§7/D08/D16） | 无 | T05 |
| 11 | joker.proto gRPC RelayMessage(is_local) + deliver worker（§3/AGENTS §2） | 无 gRPC；SendMessage 投递分支为空 | T06 |
| 12 | chat.ack + CS 消费推 ACK（§3 13-15） | 无 | T07 |
| 13 | cmd 路由（login/heartbeat/msg 注册式分发，AGENTS §6 P0） | poccessMessage 只 unmarshal 后直接 SendMessage | T05（msg 分支随 produce 落地） |
| 14 | 会话/历史/离线拉取 REST（§2 网关职责） | 无 | T05/T06 随链路补 |
| 15 | 本地 compose 全栈（§4 部署规格） | 旧 mysql+work 拓扑 | T17（验收在 T08 即起 compose 跑通） |
| 16 | feed 存储/发布/fanout/读取/动作（§6） | 无 | T09-T12 |
| 17 | groups/group_members + 管理 API + 群事件消息（§5/D09） | 无 | T13-T14 |
| 18 | @/@all 校验链（§5） | 无 | T15 |
| 19 | chat.notify + push worker（mock 厂商通道，§4/D12） | 无 | T16 |
| 20 | 生产三节点脚本（§9，写不执行） | 无 | T18 |
| 21 | 简易 Web 客户端（§6 客户端规格） | template/index.html 静态页 | T19 |
| 22 | tools/stress 骨架（不执行） | 无 | T19 附带 |

### 已知既有缺陷（T01/T02 顺带修复，均为验收门阻塞项）

- `go vet ./...` 4 处告警：`service_register.go:40` logrus 格式符、3 处 `signal.Notify` 无缓冲 channel。
- `database/user.go List` / `friend.go List`：切片按值传参，查询结果被丢弃（AddFriends 永远返回空）。T02 改写 pgx 时一并消灭。
- `http_client/client.go Combind`：硬编码 `u.Path = LoginPath`（路径拼接被登录路径劫持）。
- go.mod 未 tidy（AGENTS §5.6）：缺显式声明，随 T02 加新依赖时治理。

## 2. 任务规格索引

| 任务 | Spec | 状态 |
|---|---|---|
| T01 修既有 bug | [T01-bugfix.md](T01-bugfix.md) | 完成 |
| T02 PG 迁移 | [T02-postgres.md](T02-postgres.md) | 完成 |
| T03 网关化 | [T03-gateway.md](T03-gateway.md) | 完成 |
| T04 在线状态 | [T04-online-kv.md](T04-online-kv.md) | 完成 |
| T05 Kafka+persist | [T05-kafka-persist.md](T05-kafka-persist.md) | 完成 |
| T06 投递闭环 | [T06-delivery.md](T06-delivery.md) | 完成 |
| T07 ACK 链路 | [T07-ack.md](T07-ack.md) | 完成 |
| T08 全链路验收 | [T08-e2e.md](T08-e2e.md) | 完成 |
| T09 feed 存储+发布 | [T09-feed-storage.md](T09-feed-storage.md) | 完成 |
| T10 fanout-worker | [T10-fanout-worker.md](T10-fanout-worker.md) | 完成 |
| T11 feed 读取 | [T11-feed-read.md](T11-feed-read.md) | 完成 |
| T12 feed 动作 | [T12-feed-actions.md](T12-feed-actions.md) | 完成 |
| T13 群聊数据模型 | [T13-group-model.md](T13-group-model.md) | 完成 |
| T14 群聊投递 | [T14-group-delivery.md](T14-group-delivery.md) | 完成 |
| T15 @ 机制 | [T15-mention.md](T15-mention.md) | 完成 |
| T16 离线通知 | [T16-notify.md](T16-notify.md) | 完成 |
| T17 本地 compose | [T17-local-deploy.md](T17-local-deploy.md) | 完成 |
| T18 生产脚本 | [T18-prod-deploy.md](T18-prod-deploy.md) | 完成（写好未执行） |
| T19 Web 客户端 | [T19-web-client.md](T19-web-client.md) | 完成（浏览器双人验收未做，API 级等价） |

> 注：状态列在对应任务通过 Review Gate 后更新；最终交接报告见 [FINAL-REPORT.md](FINAL-REPORT.md)。
> T01-T16 的 spec 内均含真实验收输出与 Review 记录；T17-T19 的实现在 T08 栈上验收。

## 3. 全局设计决策（各 spec 共用，先行声明）

- **包布局**：`database/`（pgx 数据访问，构造注入）、`internal/kafka`（producer/consumer 封装）、`worker/`（persist/deliver/fanout/push/ghost 各子包）、`feed/`（feed-api 服务）、`joker/proto`（gRPC 契约）、`joker/relay`（gRPC server/client）、`web/`（静态客户端）、`deploy/`（部署物）、`tools/stress/`（压测骨架）。
- **服务间通讯**：网关→feed-api 走 HTTP（internal/http_client）；deliver→CS 走 gRPC；CS/persist/deliver 之间走 Kafka；均不互相 import 业务实现包。
- **D18 与 D06 的关系**：D18 “Joker 零写 PG” 指**业务/消息写入只在 persist**；online:{uid} kv 是 D06 明确指定由 Joker（连接持有者）唯一维护的例外，spec 中按此声明符合性。
- **群事件消息**（D09）：管理 API 在网关事务内改 groups + 预写 system_event 消息（含 seq），再 produce chat.msg（带预写 msg_id）交 persist 做 ON CONFLICT 幂等跳过 + 统一扇出，保证扇出单一路径（D19）。
- **未实现即未实现**：未读数/已读/撤回/管理员不做（D10）；D15 心跳 seq 对账为设计项（P2 路线），本轮仅实现心跳刷新时间戳 + REST 按 seq 游标补拉；LISTEN/NOTIFY 加速为可选项不实现；厂商推送只到 mock。
