# S3c · DAU 瓶颈分析与优化实战（2026-09-08）

> 上游：[s3-dau-findings.md](s3-dau-findings.md)（S3 实测）。本文 = 瓶颈定位结论 + 优化策略 + 改造记录 + 上限实测。

## 1. 瓶颈定位（全部实测背书）

**系统有效吞吐 = min(三段消费链) ≈ 117/s → DAU ≈ 1-2 万**。produce 链 3500/s 满速、
PG/CPU/内存全线空闲——瓶颈不在任何机器，在**消费模型**。

```
cs ─3500/s✓→ kafka ─→ ①persist 205/s ─→ ②deliver ~117/s ─→ ③push 单实例
                        └────── 三段同病：单条串行往返税 ~20-60ms/条 ──────┘
```

每条消息在每个消费者内的串行路径（persist 为例）：
`FetchMessage(单) → 校验 3+ 次 PG 查询 → BEGIN/INSERT/UPDATE/COMMIT(4 次往返)
→ producePush(acks=all RF=3 跨机等 2 副本) → CommitMessages(单)`。

**关键对照**：单条全路径 ~20-60ms vs **PG 批插 200 行 0.47ms**（S3 诊断期实测）——
同一件事批处理摊薄后每行成本差 3 个数量级。往返协调成本是"每条一次"的固定税，批量化除以 N。

### 已排除嫌疑（证伪记录，同样是资产）

| 嫌疑 | 排除实验 | 结果 |
|---|---|---|
| PG fsync | synchronous_commit=off + 重测 | T 不变（111→111） |
| PG 能力 | pg_stat_activity 全 idle、批插 0.47ms/200 行 | PG 无辜 |
| 消费者数量 | 2→12 实例（分区完美均分） | 111→205 非线性，人均 55→17 反降 |
| 连接池 | 24/实例 pgxpool、41 连接 | 充足 |
| CPU/内存 | 消费端、PG 全程空闲 | 纯等待型 |

## 2. 优化策略（分层，按性价比）

| 层级 | 改动 | 预期 | 成本 |
|---|---|---|---|
| **L1 批量化（正路）** | persist：批拉(N=128/200ms)→单事务批插→批 produce→批提交；deliver：批拉→并发 gRPC relay→批提交；push 同款 | 单消费者 50→2000+，系统 T 3000-6000 → **30 万 DAU 达标** | 代码 |
| L2 配置 | chat.push acks=all→1（有 D15 seq 补拉兜底）；Writer BatchTimeout 5→20ms；deliver/push 扩实例 | ×1.5-2 | 零代码 |
| L3 结构 | 分区 12→48 + 48 消费者 + pgbouncer；读写分离 | 线性，但 L1 前无意义 | 部署 |

原则：**先摊薄单条固定税（L1），再谈水平扩（L3）**——不摊薄加消费者只是线性复制低效。

## 3. 改造记录（实施后回填）

### 3.1 代码改动清单（未包含群聊/system_event 路径，语义不变）

| 文件 | 改动 | 目的 |
|---|---|---|
| `internal/kafka/producer.go` | `Producer.WriteBatch` | 批写摊薄 acks=all 跨机确认 |
| `worker/persist/persist.go` | Run 批拉(128 条/200ms)→批处理→`flushPushes` 批 produce→批 commit；新增 `handleSingleBatch`（L2，整批 5 句 SQL）；失败语义简化为整批重投（msg_id 幂等兜底） | L1 摊薄 fetch/produce/commit 三税；L2 摊薄 PG 往返 |
| `worker/deliver/deliver.go` | 批拉→fnv32 16 分片并发 relay→批 commit；**S3c StepA 追加**：送达 push 批内累积，批尾一次 `chat.ack` 批写（`SetBatchDeliveredHook`） | 两项逐条税：gRPC relay 串行 + ack 逐条 produce |
| `commands/deliverCommand.go` | 接批钩子，批量 marshal + `WriteBatch` | 同上 |
| `internal/kafka/producer.go`（Produce） | **S3c StepA 追加**：单聊 conv_id 空（首条消息客户端常态）时分区键退化为成员对 `single_key(uidA,uidB)` | 修空键单热分区（见 3.3） |

### 3.2 挖出的两个真 bug（都是实测复现后定位，非猜）

**Bug 1：L2 批路径 kv key 缺 `seq:` 前缀 → 23505 crash-loop（本次战役最大障碍）**

- 现象：干净 smoke（10 用户 storm-echo）触发 `duplicate key value violates unique
  constraint "uq_messages_conv_seq" (SQLSTATE 23505)`，persist 每 30s 崩溃重启
  （systemd Restart + kafka rejoin，每次重启把 persist 组拖进 rebalance 风暴），
  其余消费者全部分区分配被清空 → 全链冻结。
- 定位线索（诊断日志）：`nextAfter` map 的 key 比 conv_id 少前 4 字符
  （`fae5-669f-...` 32 字符 vs `86e8fae5-...` 36 字符），批内 seq 算出 0/-3/-25。
- 根因：批自增 UPSERT `INSERT INTO kv (k,v) SELECT k, ... FROM d` 把**裸 conv_id**
  写进 kv key，而全系统约定是 `seq:`+conv_id（`NextSeqTx` 同款）。RETURNING 的
  `substring(k from 5)` 按"有前缀"假设剥离，无前缀时把 UUID 前 4 位削掉 → 回填
  map 的 key 是截断值 → `nextAfter[conv]` 全 miss → seq 从 0/负数算起 → 与既有行
  撞唯一约束。
- 修复：`SELECT 'seq:' || k, ...`（一行）。修复后同批数据自排：500 条毒积压在新
  二进制下 **LAG 0、0 错误**；smoke（新号段 offset 290000）**lost=0**、echo 延迟
  p50=310ms。
- 清理：kv 里 131 个裸 conv 键删除、messages 里 234 行 seq≤0 删除、计数器
  greatest-heal、conversations.last_seq 对齐——四项校验全 0。
- 教训：批路径 SQL 必须与单条路径共用**同一份 key 构造约定**；诊断日志带
  conv#seq sample + nextAfter map 是本次 2 分钟定位的关键。

**Bug 2（架构缺陷）：produce 空键把所有消息压进单热分区**

- 现象：storm 客户端不携带 conv_id → CS 原样透传 → `key=""` → kafka Hash 把
  **全部消息投进同一分区**（实测 chat.msg 12 分区只有 P3 有流量：919 条 vs 其余
  全 0）。这同时解释了 S3"2→12 消费者只 111→205"的扩容证伪真因——不是往返税
  不摊，而是 12 个消费者只有 1 个有活干。
- 修复：单聊且 conv 为空时用 `single_key(from,to)`（字典序排序，A→B 与 B→A 同键）
  作分区键——确定性、零查库、同会话保序、键空间退化为全量会话。

### 3.3 效果梯度（全部实测）

| 阶段 | persist 稳态落库 | deliver 稳态 | 备注 |
|---|---|---|---|
| S3 基线（单条串行） | 205 TPS | ~117/s | 判据 T<1800 不达标 |
| L1 批量后（drain 测试） | 651 TPS | — | 上一会话测 |
| L1+L2 五句 SQL（S3c StepA 实测） | **≈4300-5120/s** | 230/s（未批 ack） | persist 达标判据 ✓ |
| +deliver ack 批写（StepA′ 实测） | 5120/s（CPU 竞争下） | **≈5100/s 持平 LAG** | ack 批写 22× |
| +分区键修复（12P 摊开） | ~5.3k/s | ~4k/s | 混部 CPU 集中成新墙 |

### 3.4 S3d 投递链四项修复（2026-09-08 晚，端到端验证前完成）

S3c 结束时投递链还有四个结构性问题：ack 广播串行（3500 档 ack p50≈14s）、
deliver 单实例逐条 unary、慢消费 3s 阻塞、Kafka 重试重复帧烧 seq。逐项修复：

| 修复 | 改动 | 效果 |
|---|---|---|
| **persist 批路径重试幂等** | `handleSingleBatch` 在 seq 分配前：批内按确定性 msg_id 去重 + 一次 `WHERE msg_id = ANY` 查已落库重放；重放复用原 seq 补投 push 不烧号，仅新消息进 seq 自增 | 终测 PG 新增行与客户端发送**分毫不差**（2,060,823=2,060,823），Kafka at-least-once 重试不再放大流量/烧号 |
| **ack 共享组 owner-routing** | cs-ack-{随机UUID} 广播组（每台 CS 串行吃全量）→ 稳定共享组 `cs-ack` + 批拉批提交 + `GetMany` 批查 online + 本机直写 / 异机 `BatchRelayAcks` 批转发 + not_found 刷新重试一次 | ack p50 14s → **650ms**（22×），ack 覆盖率 99.89% |
| **Batch Relay + 多实例 deliver** | proto 增 `BatchRelayMessages`/`BatchRelayAcks`（保持 Unary 取舍，逐条结果防局部失败被掩盖）；deliver 按目标 CS 地址聚批、RPC 失败逐条回退；`brave-deliver@.service` 模板 ×6 实例共享组（每台 2 个，12P 均分） | 每批每 CS 一次 RPC；relay 连接复用+失败驱逐抽到 `joker/relayclient` |
| **send_full 即时化 + 计数** | relay 写 Send 通道从"等 3s"改为立即非阻塞判定；原子计数（enqueued/send_full/not_found/marshal_error/batches）+ `BRAVE_PPROF=1` 时 `/debug/relay` JSON 端点，collect.sh 周期采集 | 慢消费不再阻塞批投递；终测全程 **send_full=0** |
| stress 工具 | 发送分片 `-send-workers`（旧单 goroutine 在 5000 档实测只达 3504/s，是工具墙不是 SUT 墙）；ack 统计从"连接最近发送"改为 per-message cli_msg_id 对账（matched/missed/覆盖率） | 高档位测量有效化；ack 延迟第一次可精确测量 |

配套运维修复：部署脚本 `pkill -f "[b]rave "` 会匹配 PG backend 命令行
（`postgres: 18/main: brave brave`）误杀数据库——锚定全路径 `^/usr/local/bin/brave `。

**测试**：relay 批入队/计数/send_full、deliver 聚批/回退/not_found 重试、
persist 批重放幂等（真实 PG：批内重复+跨批重放不烧 seq 不多落库）——单测+集成全绿。

## 4. 上限实测（2026-09-08 22:32-22:46，冻结口径终测）

**协议（预注册后执行）**：单向 storm（不跑 echo——回显把每条消息变两条 Kafka 消息，
不能当用户消息容量）、9 万连接（30k×3，自驱动=含 driver 开销的保守下界）、
目标 3500/s（1167/台，8 发送分片）、建连完成后 10 分钟、四重对账
（客户端 sent / Kafka 唯一消息 / PG 唯一新行 / 链路 LAG 斜率）。

### 4.1 终测数据（raw 归档：s3d-final-node{1,2,3}/、s3d-collect-node{1,2,3}/）

| 指标 | 值 | 对照 S3 基线 |
|---|---|---|
| 连接 | 90,000/90,000，建连/发送 0 失败 | 同 |
| 实际发送 | **2,060,823 条 / ≈3495/s**（10s 报告差分核实） | produce 3500/s |
| **PG 新增行** | **2,060,823（与 sent 分毫不差，零重试放大）** | 205,353 |
| 送达 lost | 2,295 = **0.111%**（at-least-once，补拉兜底 D15） | 投递到达率 4% |
| ACK 回执 | matched 2,058,653，**覆盖率 99.89%**，missed 2,170 | 不可测 |
| 单向延迟 | p50=460ms / p95=1.51s / p99=4.2s | p50=30s 桶顶 |
| ACK 延迟 | p50=650ms / p99=4.6s | p50≈14s（S3c 中期实测） |
| LAG | 全程亚秒级波动（persist≤1.6k/deliver≤2k/cs-ack≤1k），**跑后归零** | 线性增长无自愈 |
| relay | send_full=0 / not_found=0 / marshal=0，全程 | 不可观测 |
| crash | persist/deliver 0 退出，0×23505 | crash-loop |
| dup / PG seq | dup=0；每会话 seq 连续无空洞（smoke 抽查 5 会话 1..120） | — |
| node-1 load | 峰值 ~18/4核（自驱动含 driver），系统稳定消化 | — |

注：report 中 out_of_order 大（12-16 万）是**工具口径假阳性**——账本按 conv 全局记
lastSeq，但单聊双向消息由两个接收方交叉确认收帧，跨接收方"回退"是自然现象；
PG 无空洞 + dup=0 + 分区保序证明真实投递 per-user FIFO 成立。

### 4.2 DAU 终判（对照 stress-plan §1.3 判据表，TPS 是事实、DAU 是估值）

| 项 | 值 |
|---|---|
| 实测 T（含投递与 ACK 的端到端稳态） | **≈3495/s 单向**（自驱动保守下界） |
| 判据表落点 | T ∈ [1800, 3600] → **×10 平均口径 30 万 DAU 成立**（P99 可恶化不清盘）；×20 全包络需 T≥3600 未达 |
| ×10 口径反推 | 3495 × 172.8 ≈ **60 万 DAU**（纸面上限，非宣称值） |
| ×20 保守口径反推 | 3495 × 86.4 ≈ **30 万 DAU**（纸面） |
| 诚实宣称 | **"单向 3500/s 端到端（投递+ACK）稳定"** 是实测事实；DAU 按口径反推给区间，不宣称"30 万实时 DAU 达标" |
| vs S3 | 同硬件 205→3495 TPS（**17×**），投递从 4% 到 99.89%，ack p50 14s→650ms |

### 4.3 遗留与下一杠杆

- 8000/s 配置档实测 4521/s 即投递排队（lost 28.6%）——**当前混部拓扑的真实上限带在
  3.5k-4.5k/s**：node-1 集中了 PG+broker+cs+persist×6+driver。
- 下一杠杆是**拓扑**不是代码：PG 独立机 / 外置 driver（8C32G）后重测，预期解出
  persist 5k+/s 的完整产能；群聊扇出（D13 表中 18000 TPS 口径）本轮未测。
- echo 模式客户端账本的假丢失（relay 计数证明全部入队、PG 无损）留档：
  storm-echo 的 lost 口径不可用于容量判读，只用单向 storm。
