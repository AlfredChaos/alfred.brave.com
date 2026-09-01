# T06 · 投递闭环（joker.proto gRPC + deliver worker）

## 1. 目标与范围

**做**：
1. `joker/proto/joker.proto`：Relay 服务（Unary，取舍理由记录在 proto 注释）+ `is_local` 防回环契约位（Kafka 主干下投递单向，恒 false，保留供契约审查）。
2. `joker/relay`：gRPC server（:37012）——RelayMessage 查本机连接表 → 写 Send 通道 → WS 帧 `{cmd:"msg", data:<chat.Push>}`；查无连接回 `not_found`；Send 满 3s 超时按失败处理。
3. `worker/deliver`：消费 chat.push（group=deliver，单 goroutine 处理完才 commit）→ Router（PG kv 权威 + 30s TTL 缓存，负缓存同 TTL）→ gRPC 投递 → not_found 时强刷缓存重查重投一次（D06 防线②）；离线跳过（T16 挂 notify）；送达回调 produce chat.ack（key=from_uid）。
4. `brave deliver` 命令 + etc/deliver.yaml；gRPC 连接按 addr 懒建复用。
5. joker 启动 gRPC server（:grpc_port），优雅停机。

**不做**：CS 侧 ack 消费（T07）；chat.notify（T16）；LISTEN/NOTIFY 加速（可选不实现）。

## 2. 接口契约

- proto（详见 `joker/proto/joker.proto`）：`Relay.RelayMessage(RelayMessageRequest) → RelayMessageResponse{delivered, reason}`。
- WS 下行帧：`{"cmd":"msg","data":{"msg_id","cli_msg_id","conv_id","seq","from_uid","to_uid","type","content","mention"}}`。

## 3. 架构符合性声明

- **D01/D18**：Joker 收投递只查本机连接表，零路由决策零 PG 读；投递路由集中在 deliver（可测试可观测）。
- **D06**：三道防线落地 ①TTL 30s（含负缓存）②not-found 即时校正（测试覆盖真漂移：双 gRPC 节点）③不做。
- **D19/D20**：deliver 只消费 chat.push；单 goroutine + 处理完才 commit；key=to_uid 同接收者串行。
- **D12**：离线判定=查路由 miss，零额外查询；当前静默跳过（日志），T16 接通知。

## 4. 测试计划

- 单元：Router TTL 命中/过期回源/负缓存/Invalidate（fake kv + 注入时钟）。
- 集成（PG + 进程内双 gRPC 节点）：在线闭环（帧内容/seq 正确）；离线 no-op；缓存陈旧 → not_found → 强刷 → 重投成功。

## 5. 验收标准

门禁四项 + `go test -tags=integration ./worker/deliver/` 全绿。

## 6. 验收记录（实测粘贴）

```
--- PASS: TestDeliverClosure (0.03s)
--- PASS: TestDeliverOfflineSkip (0.01s)
--- PASS: TestDeliverStaleCacheCorrection (0.01s)  // 双节点真漂移
--- PASS: TestRouterCacheHitTTL (0.00s)
--- PASS: TestRouterOfflineNegativeCache (0.00s)
ok   alfred.brave.com/worker/deliver
```
门禁四项全绿 ✅

## 7. Review 记录

- proto 契约含 is_local 注释说明现状 ✅；deliver 地址取自 kv online:{uid}.addr（架构图提及 etcd 找 gRPC 地址，kv 即路由表，spec 声明此简化）✅
- gRPC 连接复用按 addr（无泄漏：进程生命周期持有，Close 统一回收）✅
