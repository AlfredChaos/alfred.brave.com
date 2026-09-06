# tools/stress · 压测客户端

> 配套 [../../docs/stress-plan.md](../../docs/stress-plan.md)（完整压测计划：指标/阶段/记录规范）。
> 诚实边界：本工具产出客观数据（计数/分位数/对账/CSV）；容量结论必须配合
> `collect.sh` 资源采集与真实调优后的服务器，本地数字不可外推。

## 模式

| 模式 | 行为 | 用法 |
|---|---|---|
| `hold` | N 连接限速分批建立（`-rate` conn/s）、30s 心跳，报告存活/断开 | `-mode hold -users 50000 -batch 500 -rate 1000 -duration 60m` |
| `storm` | 配对连接互发带纳秒戳消息，收端算单向延迟分位数 + 投递对账 | `-mode storm -users 200 -rate 500 -duration 10m` |
| `storm-echo` | 收端回显原始消息（text s→e，只回显一次），RTT 交叉验证 | `-mode storm-echo -users 20 -rate 100 -duration 5m` |

## 产出

- `-out <dir>/summary.csv` —— 10s 周期快照（连接/收发/失败/P50/P95/P99）；
- `-out <dir>/report.json` —— 结束汇总：全计数、单向/ack 延迟分位数（10ms 桶）、
  投递对账（sent_ids / lost / received_dup / out_of_order）。

## 关键参数

- `-gateway`：注册登录入口（建连后直连 ws_addr，符合真实客户端行为）；
- `-ws-override host:port`：强制直连指定 cs（方案 B 纯净单点压测）；
- `-seed`：账号前缀，**多机压测时每台必须唯一**；
- `-rate`：hold=建连速率上限；storm=全局发送速率；
- `-duration`：结束前有 2s 在途宽限，lost 口径=真丢。

## 采集配套

被测节点跑 `./collect.sh [outdir]`（10s 周期 CSV：CPU/内存/FD/TCP 状态/重传/cs goroutine 数，
后者依赖 cs 容器 `BRAVE_PPROF=1` 的 :6060）。

## 实现要点（踩坑记录）

- gorilla 连接**写不并发安全**：心跳/风暴/回显统一走 per-conn 互斥写（早期版本并发写直接 panic）；
- echo 只回显 `text=="s"` 的原始帧：无差别回显会形成 A↔B 无限循环雪崩（延迟假象滚到桶顶）；
- 对账只认原始帧：echo 复用 mid，计入会污染 lost/dup/ooo；
- 单向延迟依赖同 worker 时钟同源（配对在同进程内），跨机部署时严禁用该值当绝对延迟，
  跨机用 RTT/2（storm-echo）或 NTP 校时后注明误差。
