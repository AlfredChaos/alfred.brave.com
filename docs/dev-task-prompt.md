# Alfred Brave 目标架构 · 长程自主开发任务提示词

> 交付方式：将本文件全文作为任务提示词交给 coding agent。agent 应凭本提示词 + 仓库内文档自主推进全部开发，无需再次向用户确认范围。

---

## 0. 你的角色与使命

你是本仓库（`alfred.brave.com`）的自主开发 agent。使命：把 `docs/target-architecture.html`（v4，11 章）定义的目标架构落地为**可运行、可验证的代码**，并交付本地/生产两套部署物与一个简易测试客户端。

三条底线：
1. **架构文档与 ADR 是规范源**：任何实现与 `docs/target-architecture.html` / `docs/architecture-decisions.md`（D01-D22）冲突，视为不合格，打回。
2. **流程不可跳步**：每个任务必须走完 SDD→TDD→验收→Review 四阶段，任一阶段不过打回重做，不得带病前进。
3. **诚实性**（AGENTS.md §1/§7）：没跑的测试不能说跑过，没验证的行为不能说完成；所有性能数字必须来自真实运行。

## 1. 必读上下文（动手前按序全部读完，禁止跳读）

| 顺序 | 文档 | 用途 |
|---|---|---|
| 1 | `AGENTS.md` | 最高工程规范：编码风格 §3-4、工程规范 §5、bug 清单 §5.6 |
| 2 | `docs/target-architecture.html` | 目标架构 v4：总体架构 §2、聊天链路 §3、在线/通知 §4、群聊 §5、朋友圈 §6、存储 §7、Kafka §8、部署 §9、组件 §10、路线 §11 |
| 3 | `docs/architecture-decisions.md` | D01-D22：每个设计决策的"为什么"与"代价"。实现时遇到架构文档未覆盖的细节，先查 ADR |
| 4 | `docs/kernel-tuning.md` | **本阶段只读不用**（§12 明确不做内核调优） |
| 5 | 现有代码 | `cmd/` `commands/` `server/` `joker/` `internal/` `conf/` `etc/` `docker/` `docker-compose.yml`——保留其框架模式（CLI 派发、viper 中间件、gin 路由注册、包级 log、Manager/Pump 结构） |

**现状认知**：架构文档是目标设计；当前代码与目标差距巨大（MySQL、无 Kafka、投递分支为空、ticker 1ns bug）。你的工作就是按 §11 路线缩小差距，**不是重写**——单二进制 CLI 派发、配置中间件、etcd Register/Discovery 等既有框架原样沿用。

## 2. 开发流程铁律（每个任务的四阶段循环）

```
A. SDD 规范驱动 → B. TDD 测试驱动 → C. 验收 → D. Review Gate
        ↑__________________________________________|
              任一阶段不过 = 打回该阶段重做
```

### 阶段 A · SDD（Spec-Driven）

每个任务动工前，先写 `docs/specs/T<NN>-<slug>.md`，内容必须包含：

1. **目标与范围**：做什么、明确不做什么（防蔓延）
2. **接口契约**（逐字对齐架构文档）：
   - REST：路由、请求/响应 JSON、错误码（沿用 `internal/abort` + i18n）
   - 存储：完整 DDL（对齐 §7，含索引与约束）
   - Kafka：topic、key、partition 数、消费组、消息 JSON/proto 结构（对齐 §8）
   - gRPC：proto 定义（joker.proto，含 `is_local` 防回环，对齐 AGENTS.md §2）
3. **架构符合性声明**：引用本任务涉及的 ADR 编号（如 D02/D06/D19/D20），逐条说明实现如何遵守；若实现需要偏离某条 ADR，**停下来在 spec 中声明冲突并按打回处理**，不得静默偏离
4. **测试计划**：单测/集成测试点列表（对应阶段 B）
5. **验收标准**：可执行的命令 + 预期输出（对应阶段 C）

Spec 写完先自查（对照 §8 Review 清单），自查通过才进入 B。

### 阶段 B · TDD（Test-Driven）

1. **先写测试**：覆盖 spec 测试计划中每个核心行为；测试必须能在实现缺失时**真实失败**（红）
2. 跑一遍确认红，记录失败输出
3. 实现功能，最小化代码量，使测试变绿
4. 重构（保持绿）：对齐 AGENTS.md 风格，消灭重复
5. **假绿零容忍**：为通过而写的测试（删断言、捕获异常吞掉、mock 到失真）= 打回

测试分层规范：
- **单元测试**（无外部依赖）：seq 分配、幂等去重、路由缓存 TTL/失效、消息 JSON 契约、@ 校验规则、心跳超时判定
- **集成测试**（`-tags=integration`，连本地 compose 的 PG/Kafka）：落库事务、UNIQUE 幂等重放、Kafka 分区顺序、gRPC 投递闭环、群聊扇出
- **端到端验收**（阶段 C 手动/脚本执行）

### 阶段 C · 验收

1. `gofmt -l .` 为空；`go vet ./...` 通过；`go build ./...` 通过；`go test ./...` 全绿（集成测试在 compose 起好后跑）
2. 逐条执行 spec 验收标准中的命令，**粘贴真实输出**到任务报告
3. 涉及运行时行为的（WS 收发、投递、扇出），在本地 compose 环境实测，记录操作步骤与观察结果

### 阶段 D · Review Gate（打回机制）

对照下方清单逐项自查，在任务报告中给出 ✅/❌。**任一 ❌ 即打回**，回到对应阶段修改：

**架构符合性（对齐 ADR）**
- [ ] 数据流方向与 §2 拓扑一致：CS 只 produce `chat.msg`，persist 落库后 produce `chat.push`，deliver 查 kv 后 gRPC 投递（D02/D18/D19）
- [ ] Joker 是哑管道：无业务路由、无全局路由缓存、不直写 PG（D01/D18）
- [ ] 在线路由：Joker 是唯一写者（建立 upsert/断开 del），Worker 只读 + 30s TTL（D06）
- [ ] 顺序保证：业务消息带 key；消费侧**每分区单 goroutine**，处理完才 commit（D20 红线）
- [ ] seq：PG kv 事务内自增；`UNIQUE(conv_id, seq)` 存在且被测试覆盖（D08/D16）
- [ ] 群聊：投递层扇出、存储 1 份；@all 仅 owner；新成员按 joined_at 过滤（§5）
- [ ] 朋友圈：发布同步落库返回 + fanout 异步；读取 inbox+pull merge（§6）
- [ ] 未实现的就是未实现：不做未读数/已读/撤回（D10），不虚报

**编码规范（对齐 AGENTS.md §3-§5）**
- [ ] 包名小写单层；导出 PascalCase / 未导出 camelCase；无下划线包名
- [ ] 每个用日志的文件顶部 `var log = event.Log`；日志带上下文字段，无裸打印
- [ ] 错误显式处理或带上下文上抛；API 层统一 `abort.Abort*`；无 `_ = err`
- [ ] struct 带 gorm/json tag；import 三段式（标准库/三方/项目内）
- [ ] 关键模块中文注释（说明为什么，不是复述代码）
- [ ] 新组件构造函数注入依赖；**未新增任何包级可变全局**（现有全局保持现状）
- [ ] server/joker/feed/worker 之间不互相 import 业务实现包；跨服务只走 HTTP/gRPC/Kafka
- [ ] 新依赖已加入 go.mod 且 `go mod tidy` 干净

**诚实性**
- [ ] 报告中已运行/未运行的验证明确区分；无编造输出

### 打回处理

- Review 不通过 → 在任务报告中记录失败项与原因 → 修改后**重走 B/C/D**（测试可能需补）
- 发现架构文档本身矛盾/缺失 → 不自行折中：在 `docs/specs/` 下记录问题，标注 `[BLOCKED-ARCH]`，跳过该任务继续其余任务，最终报告汇总

## 3. 任务序列（严格按序；每任务走完四阶段 + commit 后才进入下一个）

### P0 · 地基（§11 路线 P0 细化）

| # | 任务 | DoD 要点 |
|---|---|---|
| T01 | 修既有 bug | ticker 1ns→按 D14 客户端上报模式重构心跳；rand.Seed 移除；`go test` 有回归测试 |
| T02 | PG 迁移 | goose(postgres) 迁移脚本（users/friends/conversations/conversation_members/messages/kv 表，对齐 §7）；pgx v5 连接层（构造注入，替换 jinzhu/gorm）；现有用户/好友 CRUD 改写并通过原行为等价测试 |
| T03 | 网关化 | `/v1/login` 返回 `{token, ws_addr}`（经 etcd 服务表选 CS）；token 鉴权中间件；cloudware 子命令删除 |
| T04 | 在线状态最小版 | Joker 建立/断开 upsert/del `online:{uid}`；GHOST 对账任务（60s）；单测覆盖状态迁移 |
| T05 | Kafka + persist | chat.msg topic（12P）；persist-worker 消费→事务落库（校验+seq+INSERT）→扇出 produce chat.push；分区单 goroutine（D20）；幂等重放测试（UNIQUE 拒绝重复） |
| T06 | 投递闭环 | joker.proto（RelayMessage, is_local）；deliver-worker 消费 chat.push→查 kv（30s TTL）→gRPC→目标 CS→Send chan→WS 帧；not-found 兜底重查重投 |
| T07 | ACK 链路 | chat.ack topic；CS 消费推发送者 `{ack, cli_msg_id}`；客户端按 msg_id 幂等 |
| T08 | 本地全链路验收 | compose 起全栈：注册→登录→拿 ws_addr→连 cs-1→向连在 cs-2 的用户发消息→对方实时收到→发送方收 ACK→PG 有落库→重放不重复 |

### P1 · 朋友圈（§6）

| # | 任务 | DoD 要点 |
|---|---|---|
| T09 | feed 存储 + 发布 | posts/feed_inbox/post_actions/post_counters 表；`brave feed` 子命令；发布 API 经网关路由，同步落库返回 post_id |
| T10 | fanout-worker | feed.fanout topic 消费；好友批量 COPY 写 inbox（500/批）；is_big_v 跳过 |
| T11 | 读取链路 | inbox + big_v pull merge → tombstone 过滤 → hydrate → cursor 分页；P99 断言不做（无压测），仅功能测试 |
| T12 | 简单动作 | 点赞/评论写入 post_actions + counters 异步聚合（最小版：定时 flush） |

### P2 · 群聊 + 通知（§5 + §4）

| # | 任务 | DoD 要点 |
|---|---|---|
| T13 | 群聊数据模型与管理 API | groups/group_members；建群/拉人/踢人/退群/改名/公告/置顶/解散（owner-only 权限矩阵测试）；群事件即消息（system_event 进 seq 流） |
| T14 | 群聊投递 | persist 事务校验（成员/@all→owner/群状态）→ 批量查 online → 逐在线成员 produce；顺序域切换（gid→uid）测试：同会话 seq 无空洞、同接收者有序 |
| T15 | @ 机制 | content.mentions/mention_all；CS 格式快检 + persist 权威校验；投递 mention 标记 |
| T16 | 离线通知 | chat.notify topic + push-worker 骨架：同人合并 60s、频控；**厂商通道不接真**（接口 + mock 实现 + 日志输出），标注"演示级" |

### 交付物 · 部署与客户端

| # | 任务 | 规格见 |
|---|---|---|
| T17 | 本地 compose | §4（下节） |
| T18 | 生产三节点脚本 | §5（下节） |
| T19 | 简易 Web 客户端 | §6（下节） |

> 压测（tools/stress）本阶段**只搭骨架不执行**：`tools/stress/` 提供连接保持与消息风暴两模式，README 写明用法，不产出任何性能数字（服务器未购，见 §5 边界）。

## 4. 本地开发部署规格（T17 · `deploy/local/docker-compose.yml`）

**原则：本地跑通即可，不做高可用，不做内核调优。**

服务清单（单节点，除 Joker）：
- `postgres:15`（单实例，数据卷，初始化 SQL 挂载）
- `etcd` 单节点
- `kafka` 单 broker（`KAFKA_HEAP_OPTS=-Xmx512m`，RF=1，topic 自动创建关闭，由 init 容器/脚本显式建 topic：chat.msg/chat.push/chat.ack/chat.notify/feed.fanout，分区数按 §8）
- `gateway`（brave start）×1、`persist`×1、`deliver`×1、`feed-api`×1、`fanout`×1、`push`×1
- **`joker` ×2**（本地双节点的唯一目的：验证跨 CS 投递）

关键实现注意：
- 两个 joker 的 WS/gRPC 端口映射宿主机 `37002/37202`、`37012/37212`；`advertise_host` 配置为宿主机可达值（默认 `127.0.0.1`），保证登录返回的 `ws_addr` 浏览器可直连
- 服务间走 compose 网络名；`PROJECT_PATH`、etc/*.yaml 模板 envsubst 沿用现有 `docker/entrypoint.sh` 模式
- healthcheck + `depends_on.condition`（PG/Kafka/etcd 就绪后再起业务）
- 一键 `make local-up / local-down / local-logs`（沿用 Makefile 风格）

验收：T08 全链路在本地 compose 上完成。

## 5. 生产部署脚本规格（T18 · `deploy/prod/` · **写好不执行**）

**背景：3 台 4C16G 服务器规格已定，服务器未购买。脚本写完整、自洽、可审阅，明确标注"未在真实环境验证"。**

按架构文档 §9 布局，交付：
- `deploy/prod/README.md`：机器清单、IP 占位符表（`.env.example`：NODE1_IP/NODE2_IP/NODE3_IP）、首次执行步骤 checklist、与 kernel-tuning.md 的衔接说明（真机到手后先做调优再压测）
- 每节点一份 compose 或统一 compose + `.env` 差异：
  - node-1：PG primary + kafka-broker-1 + etcd-1 + gateway + cs-1 + persist
  - node-2：PG replica + kafka-broker-2 + etcd-2 + gateway + cs-2 + feed-api + persist(二副本) + fanout
  - node-3：PG replica + kafka-broker-3 + etcd-3 + cs-3 + feed-api + deliver×2 + push
- PG：流复制（primary/replica 配置 + 密码/env）；Kafka RF=3、`min.insync.replicas=2`；etcd `initial-cluster` 三节点
- `deploy/prod/bootstrap.sh`：远端初始化（装 docker、分发配置、起栈）+ `deploy/prod/verify.sh`：部署后健康巡检（各端口、kafka topic 列表、PG 复制状态、etcd endpoint health）
- **明确不包含**：内核调优参数（留给 kernel-tuning.md 执行阶段）；任何性能断言

## 6. 简易测试客户端规格（T19 · `web/`，无域名、无框架）

技术：单页原生 HTML + JS（不引入 React/Vue/npm 构建），由 gateway 静态服务或独立挂载。两个文件以内为宜（index.html + app.js + style.css）。

**设置页（首个界面）**：
- 手动填写网关地址（输入框，默认 `http://127.0.0.1:37001`，存 localStorage）
- 可选：WS 地址覆盖开关（调试用；默认直接使用登录响应的 `ws_addr`）
- 连接测试按钮（GET 健康检查）

功能（对应后端各阶段完成后可分段验收）：
1. 注册/登录（拿 token + ws_addr → 建立 WS）
2. 单聊：选用户、实时收发、ACK 打勾（msg_id 幂等）、断线重连按 seq 补拉
3. 群聊：建群/拉人/发消息/@（含群主 @all 权限表现）
4. 朋友圈：发布、浏览（分页）、点赞
5. 消息按 **conv 内 seq 排序去重**（D15 客户端义务，必须实现）

验收：用两个浏览器窗口（两个账号）连**不同 joker 节点**完成单聊/群聊全流程。

## 7. Git 规范

- 本提示词即授权：**每个任务通过 Review Gate 后 commit 一次**，英文 Conventional Commits，如 `feat(joker): implement grpc delivery relay` / `test(persist): add idempotent replay tests`；一次提交一个逻辑变更
- **禁止**：push、开 PR、改远程分支、任何生产部署执行
- 提交前确认 `git status` 无本任务之外的意外改动混入

## 8. 任务报告与最终交接

每任务完成后输出任务卡：

```
## T<NN> <名称>
- Spec: docs/specs/T<NN>-*.md（架构符合性声明引用 Dxx）
- 测试: 红→绿记录；go test 输出摘要
- 验收: spec 验收命令 + 真实输出粘贴
- Review: 清单 ✅/❌（❌ 项的处理记录）
- 改动: 文件列表（新增/修改/删除）
- 遗留: 未验证项、已知限制
```

全部任务完成后输出总结：已完成任务表、端到端验收记录、**已验证/未验证清单**（部署脚本未跑、push-worker 为 mock、无性能数字等必须列出）、给用户的下一步建议（买服务器→kernel-tuning→压测）。

## 9. 明确不做（越界即打回）

1. 内核调优（服务器未购；kernel-tuning.md 只读）
2. 生产脚本的真实执行与任何性能数字宣称
3. 未读数、已读回执、消息撤回、管理员角色（D10）
4. 厂商推送通道真实对接（push-worker 只到 mock 层）
5. 引入前端框架/构建链、引入 AGENTS.md 风格之外的新模式
6. 重写既有框架（CLI 派发/viper 中间件/etcd Register 等——沿用）
7. 顺手重构与本任务无关的代码

## 10. 启动指令

第一个动作：读完 §1 全部文档 → 盘点当前代码与目标的差距清单（写入 `docs/specs/README.md`）→ 写 `docs/specs/T01-heartbeat-fix.md` → 开始四阶段循环。此后自主推进，直到 §3 任务序列全部完成或遇到 `[BLOCKED-ARCH]`。
