# tools/stress · 压测客户端骨架

> **本阶段只搭骨架，不执行**（dev-task §3：服务器未购买，不产出任何性能数字）。

## 模式

| 模式 | 行为 | 用法 |
|---|---|---|
| `hold` 连接保持 | N 连接分批建立（`-batch` 每批 + `-batch-delay` 批间隔），每连接 30s 心跳（D14 客户端模式），到 `-duration` 后输出存活/断开计数 | `go run ./tools/stress -mode hold -gateway http://127.0.0.1:37001 -users 2000 -batch 200 -duration 10m` |
| `storm` 消息风暴 | 在线连接按 `-rate`（全局 msg/s）互发单聊消息，输出发送/接收/失败计数 | `go run ./tools/stress -mode storm -users 200 -rate 100 -duration 5m` |

前置：网关可达；账号由 `-seed` 前缀批量注册（密码 Stress123，重复运行幂等）。

## 诚实边界

- 输出**只有结构性计数**（连接/心跳/收发/失败）——不生成 TPS/延迟/P99 结论；
- 性能数字必须配合 `docs/kernel-tuning.md` 的资源采集模板（CPU/内存/FD/goroutine/Kafka lag），
  在真实调优后的机器上跑出并人工记录；
- 本地 compose 单副本栈的数字**不能**外推为集群容量。
