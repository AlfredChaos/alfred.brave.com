# AGENTS.md — Alfred Brave 分布式即时通讯系统（求职练手项目）

> 本文件是面向 AI 编码助手（Claude Code / Codex 等）的工程指导，定义本项目的定位、补足目标、编码风格、工程规范与任务清单。
> 项目内规则文件与用户在对话中的明确要求，优先于本文件。

## 1. 项目定位

- **性质**：为了求职面试而准备的**练手项目**，目标是补足成一个"真正能支持百万级 WebSocket 长连接的分布式即时通讯系统"。
- **用途**：作为 Agent 应用工程师 / AI 后端工程师岗位的"Go 后端基本功 + 分布式系统"证明材料。
- **诚实性原则（最高优先级）**：
  - 所有对外说明（README、简历叙事、面试讲稿）必须与代码实现一致，不夸大未实现的功能。
  - 压测数据、性能数字必须来自真实运行，严禁编造。
  - 已实现 → 讲"实现"；设计未落地 → 讲"设计方案"；完全没做 → 不讲。
  - README 顶部必须标注本项目为"求职练手项目"，并区分"已实现"与"设计/计划中"。
- **参考来源声明**：本项目架构参考了开源项目 [link1st/gowebsocket](https://github.com/link1st/gowebsocket)（Gin + WebSocket + gRPC 分布式 IM）。但**禁止照搬其代码、命名、proto 字段、消息结构**——面试官可能识别出同款项目。借鉴其架构思想与设计模式，实现用本项目自己的设计。

## 2. 补足目标

把当前半成品补成一个端到端可验证的分布式 IM：

```
发送方 ──WebSocket──► Joker-A ──(本地查不到收信人)──gRPC──► Joker-B ──本地投递──► 收信方
                                    │
                              etcd 存储登录状态 + 服务发现
```

核心补足项：
1. WebSocket 消息投递链路（当前 `SendMessage` 本地/异地分支为空）。
2. Joker 节点间 gRPC 通讯（当前完全没有 gRPC 代码）。
3. 消息路由（cmd 字段分发：login / heartbeat / msg）。
4. 心跳超时清理（当前 `time.NewTicker(1)` 是 1 纳秒的 bug）。
5. 压测脚本 + 真实性能数据。
6. TCP/内核调优文档化（用户实际做过手动调优，需补成可复现记录）。

## 3. 编码风格规范（与原项目保持一致）

新增代码必须与现有代码风格一致，先读后写。原项目风格特征：

### 3.1 命名
- 包名：小写、单层、简洁（`etcd`、`conf`、`exchange`、`api`），不出现下划线或驼峰包名。
- 导出标识符：PascalCase（`ServiceRegister`、`UserFactory`、`ReadPump`）。
- 未导出标识符：camelCase（`putKeyWithLease`、`getServiceByRandom`）。
- 类型名用单数（`Client` 不写 `Clients`，`Manager` 不写 `Managers`）。
- 接口名：行为型用动词或 Factory 后缀（原项目有 `Factory interface { Update/Get/Delete }`）。

### 3.2 包级日志变量
每个需要日志的包，在文件顶部声明包级 log，统一走项目 logger：
```go
var log = event.Log
```
日志调用带上下文字段，禁止裸打印：
```go
log.Errorf("user %s login failed: %v", uid, err)
log.WithFields(map[string]interface{}{"user": uid, "host": host}).Info("login success")
```

### 3.3 错误处理
- 错误显式返回或带上下文上抛，**绝不静默吞掉**。
- API 层用统一 `abort.Abort*` 响应（`AbortBadRequest`、`AbortLoginError` 等），不直接 `c.JSON` 散落错误码。
- 底层封装错误用 `errorHandler(err)` 之类统一处理。
- 不接受 `_ = err` 静默丢弃，除非有明确注释说明为何忽略。

### 3.4 结构体与 tag
模型/DTO 结构体带 `gorm` 和 `json` tag，对齐原项目格式：
```go
type User struct {
    ModelBase
    UserName string `gorm:"type:VARCHAR(255)" json:"user_name"`
}
```
字段顺序：嵌入基类 → 业务字段 → 时间字段。json 字段名用 snake_case。

### 3.5 常量
包级常量用 `const ( ... )` 分组，关联常量用 `iota`，对齐 `common/constant.go`：
```go
const (
    PrefixUsers = iota + 1
    PrefixService
)
```

### 3.6 import 分组
三段式分组，空行分隔，顺序：标准库 / 第三方 / 项目内部：
```go
import (
    "context"
    "fmt"

    "github.com/gin-gonic/gin"
    clientv3 "go.etcd.io/etcd/client/v3"

    "alfred.brave.com/event"
    "alfred.brave.com/internal/etcd"
)
```

### 3.7 工具链
- `go fmt` / `gofmt` 格式化，提交前必须过。
- Go 版本：go.mod 声明 `go 1.14`，但实际可用更高版本编译；新增依赖前先 `go mod tidy` 修复现有未整理的依赖（go.mod 当前缺 etcd/websocket/bcrypt/uuid 显式声明）。

## 4. 注释规范

- **关键模块、复杂逻辑、有设计取舍的地方必须用中文注释**，说明"为什么这样做"和"做什么"，不只复述代码。
- 函数级注释用中文说明意图与职责，便于 review 与面试讲解。
- 字段、常量、简单 getter 可用简短中文或英文。
- 禁止无意义注释（如 `// 设置名字` 在 `user.UserName = name` 上方）。
- 对外契约（proto 定义、公开 API、配置项）必须注释清楚语义与边界。

示例：
```go
// SendMessage 处理一条消息投递。
// 先从 etcd 查收信人登录状态，若登录在本机则直接写入收信人 Send 通道；
// 若登录在异地 Joker 节点，则通过 gRPC 转发给目标节点。
// isLocal=true 表示本次调用来自其他 Joker 转发，本机查不到即终止，避免无限回环。
func (c *Client) SendMessage(request *MessageRequest, isLocal bool) {
    ...
}
```

## 5. 工程规范（SOLID 务实版）

SOLID 是指南针不是教条。本项目是练手项目，要在"与原风格一致"和"工程规范清晰可讲"之间取平衡——新代码体现规范，不强行重构现有可工作的全局状态代码。

### 5.1 单一职责（SRP）
- 一个文件/函数做一件事。新增功能按现有目录职责放置，不混入无关逻辑。
- 目录职责：`server/`（业务 HTTP API）、`joker/`（WebSocket IM）、`cloudware/`（服务注册中心）、`internal/`（可复用基础设施：etcd、logger、errors、i18n、http_client）、`database/`（数据访问）、`common/`（常量）、`conf/`（配置）、`event/`（日志与配置加载）。
- 新增能力若跨多个现有目录，按"最贴近的职责"归位，并在注释里说明归属理由。

### 5.2 开闭原则（OCP）
- 通过接口扩展，不修改已稳定的核心逻辑。沿用原项目 `Factory interface` 模式新增实现。
- 消息路由用注册式（`Register(cmd, handler)`），新增 cmd 不改路由分发主逻辑（参考 gowebsocket `acc_routers.go` 思路，但用自己的实现）。

### 5.3 依赖倒置（DIP）与依赖注入
- 新增模块**依赖接口，不依赖具体实现**。
- **新增组件通过构造函数注入依赖**，不新增 viper 全局懒加载式的 service locator（原项目 `dbClient()` 内部读 viper + 懒加载属于历史代码，不模仿）。
- 正确示例（构造函数注入）：
```go
type MessageDispatcher struct {
    manager   *exchange.Manager
    etcdConn  *clientv3.Client
    relayCli  RelayClient  // gRPC 转发客户端接口
}

func NewMessageDispatcher(m *exchange.Manager, conn *clientv3.Client, relay RelayClient) *MessageDispatcher {
    return &MessageDispatcher{manager: m, etcdConn: conn, relayCli: relay}
}
```
- 现有全局可变状态（`etcdConn`、`Controller`、`Services`、`ServiceHost`）保持现状不重构，但**新代码不引入新的包级可变全局**。新状态封装进结构体，通过依赖注入传递。

### 5.4 解耦
- `server` / `joker` / `cloudware` 三个服务之间不直接 import 对方的业务实现包；跨服务通讯走 HTTP client（`internal/http_client`）或 gRPC（新增），不直接函数调用。
- 可复用基础设施放 `internal/`，服务内部逻辑放各自目录。
- gRPC 节点间通讯定义独立 proto 包，joker 既作 server 又作 client，但共用同一份 proto 生成的接口。

### 5.5 并发安全
- 涉及共享状态用原项目的并发原语风格：`sync.Map`（客户端表）、`sync.Mutex`/`sync.RWMutex`（服务列表）、channel（事件分发）。
- 新增共享状态必须显式说明并发保护方式，注释指出锁/channel 的保护范围。

### 5.6 修复已知 bug（优先）
补足前先修这些既有缺陷，避免在烂地基上盖楼：
| 位置 | 问题 | 修复 |
|---|---|---|
| `joker/exchange/client.go:55` | `time.NewTicker(1)` 是 1 纳秒，心跳风暴 | 改 `time.NewTicker(30 * time.Second)` |
| `server/api/login.go:199` | 每次调用 `rand.Seed`，非并发安全 | 用 `rand.Intn`（Go 1.20+ 自动 seed）或 `sync.Mutex` 保护 |
| `go.mod` | 未 `go mod tidy`，缺 etcd/websocket/bcrypt/uuid 显式声明 | 跑 `go mod tidy` |
| `server/api/register.go` | `VerifyUserName/Email/Password` 全是空函数 | 实现基础校验 |

## 6. 补足任务清单（分阶段）

每个任务独立可验证，完成一块能编译运行验证后再做下一块。

### P0（核心链路，必须完成）
- [ ] **修既有 bug**（见 5.6）
- [ ] **本地消息投递**：`SendMessage` 收信人在本机时，从 `Manager` 取 client 写入其 `Send` 通道。
- [ ] **gRPC 跨 Joker 通讯**：
  - 新增 `joker/proto/joker.proto`，用 **Unary RPC**（不用 Stream，理由：消息模型是离散的，Unary 实现简单、易调试；面试可讲"从设计时的 Stream 改为 Unary"的取舍）。
  - 定义 `RelayMessage`（含 `isLocal` 字段防回环）、`QueryUserOnline`。
  - Joker 启动 gRPC server，收 `RelayMessage` 后本地投递。
  - gRPC client 连接池：从 etcd 服务发现列表为每个 peer Joker 建立连接。
  - `SendMessage` 异地分支：查 `LoginHost` → 取对应 Joker 连接 → 调 `RelayMessage`。
- [ ] **消息路由**：`poccessMessage` 按 `cmd` 字段分发到 login/heartbeat/msg 各 handler，用注册式路由。
- [ ] **心跳清理**：定时任务清理超时连接（参考 6 分钟无心跳断开）。
- [ ] **压测脚本**：`tools/stress/` 下 WebSocket 压测客户端（N 个连接、分批建立、保持存活），记录连接数/CPU/内存/失败率/维持时长。配套工具可用 [link1st/go-stress-testing](https://github.com/link1st/go-stress-testing)。

### P1（工程完整度）
- [ ] **TCP/内核调优文档**：`docs/tcp-tuning.md` 记录 sysctl 参数、ulimit、tcp_mem/rmem/wmem，附可执行脚本。
- [ ] **cloudware 处理**：当前 `service_register.go`/`service_list.go` 是 `c.JSON(200,nil)` 空壳。决策：要么补完实现成 HTTP 注册中心，要么删除改用 Nginx upstream 负载方案（在文档说明取舍）。
- [ ] **API 参数校验**：实现 `VerifyUserName/Email/Password`。
- [ ] **go.mod 依赖整理**：`go mod tidy` 后显式声明所有直接依赖。

### P2（加分项）
- [ ] **离线消息**：收信人不在线时落库，上线推送（最简版）。
- [ ] **消息投递语义**：明确并实现 at-least-once，文档说明失败与重复处理。
- [ ] **单元测试**：核心逻辑（消息路由、投递分发、etcd UserFactory、服务发现列表维护）补 `*_test.go`。原项目 0 测试，至少补核心路径。

## 7. 诚实性与对外说明规范

- README 顶部标注：
  > 本项目为**求职面试练手项目**，用于实践分布式即时通讯系统设计。架构参考 link1st/gowebsocket，使用 Gin + WebSocket + gRPC + etcd。下文区分【已实现】与【设计/计划中】。
- 简历 bullet 与代码实现一一对应，每条能指到具体文件。
- 压测数据按实际硬件诚实写。**16GB 内存单机承载不了 100 万 WebSocket 连接**（每连接约 27KB，100 万需 ~25GB，参考 gowebsocket 压测数据）。若硬件受限，写"单机 X 万长连接"的真实数字，比"百万"更可信。
- 简历原"百万长连接 / gRPC Stream / TCP 调优"叙事校准：
  - "gRPC Stream" → "gRPC（Unary）"，与主流 Go IM 实现一致。
  - "百万长连接" → 仅在真跑出该数字时保留，否则写实际值。
  - "TCP 调优" → 用户实际做过手动调优，补文档化后可保留。

## 8. 验证规范

- 每补一块，先确认能 `go build ./...` 通过，再验证运行时行为，不接受"应该能跑"。
- 核心逻辑（消息投递、gRPC 转发、路由分发）写单元测试或可复现的手动验证步骤，记录在任务对应的注释或 docs。
- 压测必须记录：硬件配置、压测命令、连接数曲线、CPU/内存/FD/goroutine 峰值、断线率、维持时长。
- 不可逆操作（删除文件、重写模块）先确认再动手，不触碰与当前任务无关的代码。

## 9. Git 与提交规范

- 仅在用户明确要求时才 commit / push / 提 PR。
- 提交信息用英文 Conventional Commits：`<type>(<scope>): <subject>`，一个提交一个逻辑变更。
- 示例：`feat(joker): implement local message delivery in SendMessage`、`fix(exchange): correct heartbeat ticker interval`。
- 多阶段任务在阶段边界提交一次。

## 10. 工作方式备忘

- 先读后写：动手前查相关代码、配置、测试，核实路径与约定。
- 信息缺口影响方案方向时，先向用户提问，一次问全；次要歧义按合理假设推进并声明。
- 改动接口或跨层能力时，追踪上下游影响面：定义、封装、调用方、测试、文档。
- 如实报告：没跑的测试、没读的文件、没验证的结论，不能说成做过。

## 11. 分布式 IM 面试参考

分布式即时通信系统的面试问题、详细回答、设计取舍，以及与当前 Brave 代码状态的对应关系，统一维护在：

- [docs/distributed-im-system-design-interview-reference.md](docs/distributed-im-system-design-interview-reference.md)

涉及 WebSocket 长连接、在线路由、etcd lease、消息 ACK、幂等去重、会话内顺序、离线补拉、跨 Joker relay 或群聊 fanout 时，先阅读该文档。文档中的“目标架构”不等于“已实现能力”，实现状态以代码和本文档的“当前代码状态”章节为准。
