# T04 · 在线状态最小版（online kv + GHOST 对账）

## 1. 目标与范围

**做**：
1. Joker 连接生命周期维护 PG kv：WS 建立 `upsert online:{uid} = {cs, addr, ts}`；断开 `DeleteIfMatch(cs=自己)`（漂移保护：旧节点晚到的 del 不删新节点的记录）。
2. GHOST 对账任务（60s 周期）：扫 `online:*` → 解析 cs → 不在 etcd 存活服务表 → 删除。`brave ghost` 子命令承载。
3. 移除 etcd 用户态：joker /login /logout HTTP 端点、etcd.UserFactory、exchange 里的 etcd 登录态检查（msg handler 暂回 OperationFailure，T05 由 chat.msg 链路取代）。etcd 回归纯服务注册表（D05/D06）。
4. joker 配置：新增 postgres 段与 grpc_port（T06 gRPC server 用，本任务先把地址写进 kv）。

**不做**：Worker 侧 30s TTL 读缓存（T06 deliver 实现时落地）；LISTEN/NOTIFY（可选项不实现）；D15 心跳 seq 对账（P2）。

## 2. 接口契约

- kv 记录：`online:{uid}` → `{"cs":"<ws host:port>","addr":"<advertise:grpc_port>","ts":<unix>}`
  - cs 必须与 etcd services 表 value 同格式（host:port），GHOST 对账据此比对。
- `brave ghost`：循环（60s）{ 刷新 etcd 服务表（watch 常驻）→ ScanPrefix online: → 删 GHOST }，日志输出每次清理数量。
- 状态机（§4）：OFFLINE→ONLINE（upsert）/ ONLINE→OFFLINE（条件 del）/ ONLINE→GHOST（CS 宕机残留）→OFFLINE（对账清理）/ 漂移（重连新 CS 覆盖 upsert，旧 del 不命中）。

## 3. 架构符合性声明

- **D06**：Joker 是 online kv 唯一写者；权威单点 PG；本任务后 etcd 只剩 services/*。
- **D18 澄清**：“Joker 零写 PG”指业务/消息写入；online kv 是 D06 指定给连接持有者的唯一例外（README/specs 已声明）。
- **D14 关联**：CS 宕机 → 连接死 → kv 残留 → GHOST 由对账任务清理，窗口最坏 ≈ lease(60s)+扫描(60s)。

## 4. 测试计划

- 单元（fake 注入）：注册→upsert 调用参数正确；断开→条件删除带自身 cs；无 online 依赖（nil）时本地模式不报错；漂移语义由 T02 的 DeleteIfMatch 集成测试覆盖。
- GHOST：dead cs 清理、alive cs 保留、非 online 前缀不受影响、解析失败记录跳过并告警。
- 集成（-tags=integration）：真实 PG 上 online upsert/条件删/对账清理闭环。

## 5. 验收标准

```bash
gofmt -l . && go vet ./... && go build ./... && go test ./...
BRAVE_PG_DSN=... go test -tags=integration ./database/ ./worker/ghost/ -v   # 全绿
```

## 6. 验收记录（实测粘贴）

- 四门禁全绿 ✅（见任务报告）
- 集成：`ok alfred.brave.com/worker/ghost`（ghost sweep 闭环）+ `ok alfred.brave.com/database` ✅

## 7. Review 记录

- Joker 唯一写者 ✅；对账只删不活记录 ✅；msg 分支声明为 T05 前的空窗（不虚报）✅
- 新组件（OnlineTracker/Ghost Reconciler）构造注入 ✅；`brave ghost` 不注册服务表（无人发现它）✅
