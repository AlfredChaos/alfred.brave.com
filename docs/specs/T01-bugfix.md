# T01 · 修既有 bug（心跳/rand.Seed/校验空函数/vet 告警）

## 1. 目标与范围

**做**：
1. `joker/exchange/client.go:55` `time.NewTicker(1)`（1ns 心跳风暴）→ 按 D14 客户端上报模式重构：移除服务端主动 ping；客户端 30s 应用层心跳；CS 维护 `lastActive` 时间戳；Manager 定时（1min）清理 6 分钟无心跳连接；ReadPump 读超时 6min 兜底。
2. `server/api/login.go getServiceByRandom`：移除每次调用的 `rand.Seed`（Go 1.20+ 全局源已自动播种，显式 Seed 并发不安全）。
3. `server/api/register.go VerifyUserName/Email/Password` 空函数 → 实现基础校验。
4. `go vet` 既有 4 处告警（阻塞验收门 C）：`internal/etcd/service_register.go:40` logrus 格式符；3 处 `signal.Notify` 无缓冲 channel。
5. 连接生命周期补全：ReadPump 退出时发 `Unregister`（当前 Unregister channel 从未被触发，断开连接残留内存）。

**不做**：消息路由完整实现（T05）、SendMessage 投递分支（T05/T06）、online kv（T04）、心跳 seq 对账（D15，P2 设计项不实现）。

## 2. 接口契约

- WS 帧（T01 起定型，后续任务沿用）：
  ```json
  {"cmd": "heartbeat", "data": {}}
  ```
  - `cmd ∈ {login, heartbeat, msg}`；未知 cmd 回 `{"code":1001,"code_msg":"参数不合法"}`（ParameterIllegal）。
  - `heartbeat`：刷新 lastActive，回 `{"code":200,"code_msg":"Success"}`。
  - `login`：占位 ack（同上），身份仍由 URL `/ws/:id` 提供（沿用现状）。
  - `msg`：T01 内沿用现有 `SendMessage`（etcd 登录态检查，投递分支仍空，T05 重写）。
- REST 校验规则（register/login 共用）：
  - `VerifyUserName`：3-32 字符，允许字母/数字/下划线/连字符/中文。
  - `VerifyEmail`：`net/mail.ParseAddress` 合法且含 `@`，长度 ≤254。
  - `VerifyPassword`：8-64 字符，至少一个字母和一个数字。

## 3. 架构符合性声明

- **D14（心跳被动模式）**：服务端不再每 30s 主动 ping（删 WritePump ticker），只更新时间戳 + 定时清理，6 分钟超时（`heartbeatExpirationTime = 6*60s`）。代价：离线检测延迟最坏 6 分钟（T16 通知系统补位）。
- **D20 环节④**：目标端 FIFO Send chan 保持（满则踢线不跳跃）——Close 幂等化后语义不变。
- 不新增包级可变全局；`Client` 增加 manager 回引通过构造函数注入（AGENTS §5.3）。

## 4. 测试计划（阶段 B）

单测（无外部依赖）：
- `heartbeat_test.go`：心跳超时判定（lastActive 超 6min 判超时/未超不踢）；Manager 清理只踢超时连接；Unregister 触发后从连接表移除。
- `validation_test.go`：用户名/邮箱/密码 合法与非法用例表。
- `service_test.go`：getServiceByRandom 空列表返回 ""（不 panic）、非空列表返回成员之一。

## 5. 验收标准（阶段 C）

```bash
gofmt -l .          # 空
go vet ./...        # 无告警
go build ./...      # 通过
go test ./...       # 全绿
```

## 6. 验收记录（阶段 C 实测粘贴）

- `gofmt -l .` → 空输出 ✅
- `go vet ./...` → 无输出 ✅
- `go build ./...` → exit 0 ✅
- `go test ./...` → `ok alfred.brave.com/joker/exchange 0.3s`、`ok alfred.brave.com/server/api`、其余 ok，0 失败 ✅（完整输出见任务报告）

## 7. Review 记录

- 架构符合性：D14 ✅（被动心跳+6min 清理）；D20 环节④ ✅；无新增包级可变全局 ✅
- 编码规范：包级 log ✅；错误带上下文 ✅；中文注释说明取舍 ✅；无 `_ = err` ✅
- 诚实性：msg 分支仍为旧逻辑（etcd 登录态检查 + 空投递），已在“不做”声明 ✅
