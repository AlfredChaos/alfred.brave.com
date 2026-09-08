# 压测计划 · Brave IM 三节点负载上限挑战

> 配套文档：[kernel-tuning.md](kernel-tuning.md)（先调优后压测）· [../deploy/prod/README.md](../deploy/prod/README.md)（部署脚本）
> 诚实原则：本计划描述**将要执行的**压测方法与记录规范；所有容量数字以真实运行产出为准，
> 本文档中的预估仅用于设定停止条件与容量假设校准点。

## 0. 目标与非目标

**目标**（按优先级）：
1. **连接容量**：测出三节点拓扑下 cs（joker）长连接承载上限与拐点（内存/CPU 红线）；
2. **消息吞吐与延迟**：满连接水位下，单聊消息全链路（WS → Kafka → persist → deliver → gRPC → WS）的吞吐上限与 P99 延迟；
3. **稳态健康**：满水位浸泡下无资源泄漏（FD / goroutine / RSS 曲线平稳）；
4. **故障韧性**（加分）：压测中 kill 一个 cs，观察重连风暴与自愈时间（验证 Run 维持循环、deliver 重试、客户端补拉）。

**非目标**：
- 不测朋友圈/群聊大 fanout 的极限（列入后续，方法同 storm 但目标为 fanout 倍增）；
- 不产出"百万连接"宣称——3×4C16G 且压测机混部，物理上不可能（见 §2 容量预算）；
- 不做异地网络损伤测试（三台同机房/同交换机，RTT ≈ 0.2ms，记录网络前提即可）。

## 1. 被测拓扑与压测角色

### 1.1 拓扑设计（双拓扑编排，2026-09-06 定稿）

两条容量轴各有最优解：**连接吃 cs 的内存，消息吃中间件的 CPU/IO**。据此按阶段切换拓扑：

| 拓扑 | 构成 | 用于阶段 | 数字语义 |
|---|---|---|---|
| **2+1'（加权三 cs）** | node-1/2：cs 独占（~14G 给 cs）；node-3：cs-3（~5G）+ 全部中间件（PG/kafka/etcd/gateway/workers） | **S1 连接爬坡 / S2 浸泡** | 纯连接容量（65–75 万在线） |
| **混部（prod compose 原样）** | 每台 = 1 etcd + 1 broker + 1 cs + worker 分摊，PG 一主两从 | **S3 消息风暴 / S5 故障** | 消息吞吐与 DAU 口径 |
| 驾驶舱 | 本机 Mac：driver 控制台 + 3–5 万跨网补充流量（验证真实跨机链路）+ 数据汇总 | 全程 | 不直接产生容量数字 |

**压测客户端放置**（连接轴）：node-1/2/3 各跑一个本机 stress worker **走回环打自己的 cs**（不占网卡、内存与 cs 互补挤压）；Mac 打跨网流量。三台 seed 必须唯一。

**CS 选点控制**（连接轴必做）：gateway 的 `getServiceByRandom()` 均匀随机，对 2+1'
的加权容量（27/27/15 万）是错配，且会把连接甩到别的节点破坏回环设计——**每台
worker 必须 `-ws-override 127.0.0.1:37002` 直连本机 cs**，分配从随机变为确定性权重：
`-users` 按 270000/270000/150000 配。登录照常走 gateway（拿 token）；online:{uid}
由实际连上的 cs 自己 upsert，路由以真实连接为准，覆盖 ws_addr 不影响投递正确性。
（混部 S3 无需覆盖：80 万账号随机分三台 σ≈374，偏差 <0.5%，天然均衡。）

**2+1' 的原理与边界**：
- 原理：纯连接场景中间件几乎不干活（心跳不进 Kafka，PG 仅登录时写一次 kv），其要价只是 ~6.5G 驻留内存——剩余全部换算成连接，比整台让给中间件（2+1）多榨 12–18 万；
- 边界①：**纯连接专用**——node-3 上 cs-3+PG+broker+全 worker 挤 4 核，跑消息必先死，故 S3 切回混部；
- 边界②：node-3 是全家桶单点（挂一台=一半容量+全部存储），实验拓扑可接受，记录取舍；
- 边界③：真机阶段需从 prod compose 派生 2+1' 变体（cs-3 加 `mem_limit` 防 OOM 杀手误杀 PG/kafka、kafka heap 已限 512M、PG shared_buffers 显式配 512M–1G）——列入 §4.4 checklist。

### 1.2 容量预算（设定停止条件的依据，全部待实测校准）

| 项 | 预估 | 依据 |
|---|---|---|
| cs 服务端每连接内存 | ~30KB（gorilla conn + 2 goroutine + Send 通道） | gowebsocket 实测 ~27KB + 本项目余量 |
| 压测客户端每连接内存 | ~40KB | Go 客户端经验值；单 Linux 客户端 ~6 万连接上限（kernel-tuning §5） |
| **2+1' 三台合计目标** | **65–75 万连接** | node-1/2 各 25–30 万（独占 14G）+ node-3 白捡 12–18 万（~5G） |
| 混部连接上限 | 30 万（3×10 万） | cs 仅 3G 预算（架构 §9） |
| 消息吞吐瓶颈假设 | persist→PG 落库先饱和：混部 1.5–3k TPS、2+1 1.2–2.5k TPS（NVMe 本地盘；云盘减半） | 每消息一次同步事务；本地冒烟已实证瓶颈形态 |

### 1.3 DAU 容量推演（消息轴，挂 S3 验证）

按 D13 模板（人均 50 条/天，群扇出 ×5，投递/ack 随动），**峰值系数分两档**：×20（文档保守包络）/ ×10（业界常见）：

| 口径 | 混部 DAU 上限 | 2+1 DAU 上限 |
|---|---|---|
| 峰值 ×20 | **~17 万**（13–26 万） | ~15 万（10–22 万） |
| 峰值 ×10 | **~35 万**（26–52 万） | ~30 万（20–43 万） |

- 混部略优 10–15%：消息瓶颈在中间件节点，混部把 broker/worker 摊三台，2+1 反而全挤 node-3（cs 省的 CPU 被集中化的 worker 吃回）；
- **S3 验证性预言**：实测混部落库稳态吞吐 T → T≥3.6k：×20 全包络成立；T∈[1.8k,3.6k]：平均口径成立+×10 峰值可存活（P99 恶化不清盘）；T≈几百：DAU 宣称降档至 10 万级。纸面预测 T 落在 **1.5k–3k** 区间概率最大——实测打脸即回修瓶颈模型；
- DAU 优化路径（若要抬上限，按性价比）：persist 批量落库/组提交（×3–5）> PG 读写分流 > deliver 12P→24P。

### 1.3.1 压测结论判读标准口径（S3c/S3d 教训固化，后续报告必须遵守）

**TPS 是事实，DAU 是估值**——容量结论只能从实测 T 反推，禁止用 DAU 目标倒推 TPS 当达标线：
- 反推公式：DAU = T × 86400 ÷ (人均条数 × 峰值系数)。人均 50 条时 ×20 口径 = T×86.4，×10 口径 = T×172.8。
- 四条链路（persist 落库 / 在线投递 / 群扇出 / 离线通知）各有独立瓶颈与扩容路径，**不得混算**：persist 达标不代表投递达标，必须分别给出实测值。
- **速率以客户端计数为准**（10s 报告差分）；Kafka offset 斜率受批提交/重试影响只做交叉参考。配置 `-rate` 不是实际速率（S3c 实测旧发送器 5000 档只达 3504/s）。
- **只跑单向 storm 做容量判读**：echo 模式把每条消息放大成两条 Kafka 消息（不是人均 50 条的口径），且其客户端 lost 统计有已知假丢失（relay 计数证明全部入队）。echo 仅用于正确性冒烟。
- 有效性四重对账：客户端 sent ≈ Kafka 唯一消息数 ≈ PG 唯一新行数，且三段消费 LAG 不持续增长——对不上先查测量/重试，禁止继续加档。
- 自驱动（driver 与 SUT 同机）结果标注为**含 driver 开销的保守下界**；外置 driver 后才可称纯 SUT 上限。

### 1.4 端口与地址

- prod compose 起后：每台 `cs-N:37002(WS)/37012(gRPC)`，gateway 37001，压测 worker 经 gateway 注册登录拿 ws_addr 后直连；
- 每台节点暴露 pprof 观测端口（cs=6060，gateway=6061，见 §4.3）。

## 2. 查验指标（分四层）

### 2.1 连接层（压测客户端产出）

| 指标 | 定义 | 采集 |
|---|---|---|
| 建立成功率 | 建连成功数 / 尝试数 | stress 计数 |
| 建立耗时 P95 | dial→open 完成 | stress 计时桶 |
| 存活率 | duration 末仍在线 / 峰值连接 | stress 计数 |
| 断开率 | 读循环出错数 / 峰值连接（浸泡期） | stress 计数 |
| 心跳往返 P99 | heartbeat 发→无错间隔（间接健康度） | 连接存活反推 |

### 2.2 业务层（核心：延迟 + 投递正确性）

| 指标 | 定义 | 采集 |
|---|---|---|
| 端到端延迟 P50/P95/P99 | 消息 text 内嵌发送纳秒戳，**同 worker 配对连接**互发：A 发→B 收，单向延迟 = B 收到时本地钟 − 网络携带戳（同机 worker 时钟同源，精确） | stress 延迟直方图（10ms 桶，上限 30s） |
| RTT（交叉验证） | A 发→B 收→B 立即回显→A 收，RTT/2 对照单向值 | storm-echo 模式 |
| 投递成功率 | B 实收 msg_id 数 / A 已发送数（按 cli_msg_id 对账，容忍重投但不容忍丢失） | stress 计数 + msg_id 集合 |
| 重复投递率 | B 重复收到同 msg_id 的比例（验证 at-least-once 语义边界） | msg_id 集合 |
| 有序性抽样 | B 收到的 seq 单调递增（按会话） | 抽样断言 |
| ack 延迟 | 发送方发出→收到 ack 帧间隔 | stress 计时 |

### 2.3 资源层（每节点，collect.sh 采集，10s 周期）

| 指标 | 红线（达到即停/降档） |
|---|---|
| 节点 RSS 使用率 | > 85%（16G → 剩 2.4G 缓冲） |
| 节点 CPU（user+sys） | 持续 > 90% 超 60s |
| swap 换入换出 | > 0（调优后不应发生，发生即记录异常） |
| cs 容器 FD 数 | > ulimit 的 80% |
| cs goroutine 数 | ~2×连接数 ± 10%（偏离即泄漏嫌疑） |
| netstat ESTABLISHED | 与压测端连接数差 > 5% 即对账 |
| 重传/丢包 | retrans 增速突增即记录 |

### 2.4 中间件层

| 指标 | 采集 | 异常阈值 |
|---|---|---|
| Kafka 消费 lag（chat.msg/chat.push/chat.ack） | kafka-consumer-groups --describe | lag 持续增长 > 1万 或 不收敛 |
| PG 连接数 / 慢查询 | pg_stat_activity | 连接 > 池上限 80%；>500ms 查询出现 |
| etcd lease 数 / watch 延迟 | etcdctl endpoint health + 日志 | 注册丢失（对应 cs 连接数） |

### 2.5 拐点判定（全局停止条件）

任一触发即当前阶梯终止、记录拐点、退一档稳态运行：
1. 业务错误率 > 1%（建连失败或投递失败）；
2. 单向延迟 P99 > 5s 持续 30s；
3. 资源红线（§2.3 任一）；
4. Kafka lag 发散（消费速率 < 生产速率且差值扩大）。

## 3. 压测方法（阶段设计）

每个阶段有唯一编号（S0–S5），数据落 §5 目录。**每阶段前固定采集 60s 基线**。

### S0 工具链冒烟（必须先过）
- 100 连接 hold 3min + 10 连接 storm 100msg/s × 2min；
- 通过标准：投递成功率 100%、延迟 P99 < 200ms（本地/内网）、collect.sh 产出完整 CSV、延迟分位数输出正确。
- **本阶段已在本地 compose 栈执行过**（见 §5 冒烟记录），真机到手后重复一次即可。

### S1 连接爬坡（hold，阶梯加压，**2+1' 拓扑**）
- 部署：切换到 2+1' 变体 compose（cs-3 带 mem_limit）；
- **账号预注册**：按 §4.5 用 roster 模式直写 PG（API 注册因 bcrypt 不可行）；建连限速 500–1000/s，75 万连接约 12–20 分钟建满；
- 阶梯：每台 worker 以 1000 conn/s 建连，每档 15 万（三台合计），档间稳态 5min 采集；
- 15 万 → 30 万 → 45 万 → 60 万 → 65 万 → 直到触发 §2.5 拐点；
- 记录：每档末快照（连接数/内存/FD/goroutine）→ **连接-内存斜率**（KB/连接）与 node-1/2 vs node-3（加权 cs）的斜率对比。

### S2 满水位浸泡（hold，**2+1' 拓扑**）
- 取 S1 拐点的 80% 连接数，维持 60min，仅心跳 + Mac 的 3–5 万跨网连接；
- 通过标准：断开率 < 0.5%、RSS 增速 < 1%/10min、goroutine 稳定、FD 无泄漏。

### S3 消息风暴（storm，双档，**切回混部拓扑**）
- 重新部署 prod compose 原样拓扑（消息瓶颈在中间件，混部消息吞吐优于 2+1'，见 §1.3）；
- 固定连接 = 9 万（DAU 30 万×30% 在线的对应水位），配对互发；
- 吞吐阶梯：100 → 500 → 1k → 2k → 5k msg/s（全局），每档 10min；
- 每档记录：吞吐实际达成值、单向延迟分位数、ack 延迟、Kafka lag 曲线、PG 慢查询；
- **落库稳态吞吐 T 对照 §1.3 判据表** → 得出 DAU 宣称口径；
- 变体 storm-echo（RTT 交叉验证）在拐点档补跑 3min。

### S4 混合真实负载（可选，时间允许，**混部拓扑**）
- profile：70% 连接静默心跳 + 25% 低频单聊（1msg/min）+ 5% 高频单聊（1msg/s）；
- 30min，观察与 S3 纯风暴的延迟差异（读写混合的排队效应）。

### S5 故障韧性（加分，压测中注入，**混部拓扑**，S3 水位下执行）
- 满水位稳态运行中，`docker stop cs-2`：
- 观察指标：该节点连接断开数、客户端重连风暴峰值（重连 QPS）、重连成功时间、GHOST 清理条数、投递失败-恢复时长（deliver 日志 evict/retry/drop）、etcd 重注册延迟；
- 通过标准：5 分钟内重连率 > 95%，无人工干预。

## 4. 工具准备

### 4.1 tools/stress 压测客户端（已增强，随本计划交付）

| 能力 | 说明 |
|---|---|
| hold / storm / storm-echo 三模式 | 连接保持 / 单向风暴（延迟戳）/ 往返回显（RTT） |
| 延迟直方图 | 10ms 桶至 30s，每周期输出 P50/P95/P99/max |
| 投递对账 | cli_msg_id 集合，报告 丢失/重复/乱序 计数 |
| CSV 落盘 | `-out` 目录：`summary.csv`（10s 周期快照）+ 结束时 `report.json` |
| 多目标 | `-ws-override` 指定直连某 cs（单点纯净压测用） |
| 限速建连 | `-rate` 控制建连速率，防压测机自身 SYN 风暴 |

用法示例：
```bash
# 混部：三台各自跑（seed 区分账号空间）
go run ./tools/stress -mode hold  -gateway http://<nodeN>:37001 -seed n1 -users 50000 -batch 500 -rate 1000 -duration 60m -out results/n1-hold
go run ./tools/stress -mode storm -gateway http://<nodeN>:37001 -seed n1 -users 25000 -rate 2000 -duration 10m  -out results/n1-storm
```

### 4.2 collect.sh 资源采集（tools/stress/collect.sh）

在**每台被测节点**运行，10s 周期追加 CSV：node 级（CPU/RSS/swap/FD/netstat 分状态/retrans）
+ 容器级（docker stats --no-stream）+ 可选 pprof goroutine 数。结束 Ctrl-C 自动 `collect-summary.json`。

### 4.3 服务端观测端口（env `BRAVE_PPROF=1` 启用）

> 已修复：GHOST 对账原为单页 1 万条硬上限——75 万在线时漏掉 99% 残留记录。
> `kv.ScanPrefix` 改 key 游标分页 + Sweep 循环扫尽（单测覆盖 1200 行 × 500/页 多页路径）。

- joker：`localhost:6060/debug/pprof`（goroutine/heap 数，collect.sh 拉取）；
- gateway：`localhost:6061`；
- prod compose 已注入该 env（见 deploy/prod docker-compose.prod.yml override）。

### 4.5 账号账簿（roster，已交付并本地验证）

**账簿规范**（密码常量与命名规则的唯一权威出处）：

| 项 | 值 | 出处 |
|---|---|---|
| 密码（全部压测账号共用明文） | `Stress123` | tools/stress/roster.go `rosterPassword`（stress 客户端同常量） |
| 哈希代价 | cost4（仅压测池；生产注册 cost10） | roster.go `rosterBcryptCost` |
| 用户名模板 | `<seed>-<7位序号>`，如 `n1-0000123`（seed=机器段，多台唯一） | roster.go `seedUsers` |
| 账簿 CSV | `user_name,uid,email` 三列；**不含密码**（共用明文在代码常量里，不入文件/git） | roster.go `writeRosterCSV` |
| 真机产物路径 | `docs/stress-results/<date>-<phase>/roster/roster.csv`（随记录归档） | §5 记录规范 |
| 本地池示例 | `rt-0000000..rt-0000499`（本地栈冒烟）；手动验证账号 alfredo/beibei/chaos-01/chaos-2 密码 `passw0rd1` | S0 findings.md |

65–75 万连接 = 同等数量预注册账号。**不能走 API 注册**：bcrypt cost10 每次 ~50–100ms CPU，
80 万次注册要 5–11 小时，且登录验证同样吃 bcrypt——建连吞吐会被卡死在 ~50–100/s，
阶梯压测无法进行（2026-09-06 核算发现，已写入 S1 预注册项的根因）。

方案：`-mode roster` 直写 PG 批量造号 + **bcrypt cost4 哈希**（登录验证 ~1ms，
gateway 登录吞吐回千级）：

```bash
# 三台各自造本机段（seed 唯一）；幂等可重跑；本地实测 500 号 23ms
go run ./tools/stress -mode roster -dsn postgres://...  -seed n1 -users 300000 -out results/n1-roster
```

- 全部账号共用明文 Stress123 → 预计算一个 cost4 哈希，COPY 批插 + ON CONFLICT 幂等；
- 产出 `roster.csv`（name,uid,email），**不入 git**（.gitignore: roster*、/tmp 或 results/）；
- 诚实边界：压测专用账号池，绕过注册 API（注册路径已有 e2e 覆盖）；cost4 仅压测账号，
  生产注册仍是 cost10；
- stress 客户端天然兼容：registerAndLogin 幂等（注册 409 忽略 → 直接登录），
  本地已验证 roster 账号 100 连接 hold 全通；
- 登录自检：造号后抽 3 个账号过 gateway /v1/login 验证哈希有效（本地 3/3 通过）。

### 4.6 真机执行前置 checklist

1. [ ] systemd 形态部署（deploy/systemd/README.md）：build.sh 打包 → 三台 install.sh 按角色 →
   node-1 原生 PG（shared_buffers 768MB / max_connections 300）→ brave-migrate → 中间件
   集群 compose（etcd×3 + broker×3）→ 建 topic（RF=3）→ 起业务；verify.sh 逐项巡检；
2. [ ] 三节点执行 kernel-tuning.md §2–§5 sysctl/ulimit —— **按节点分档**（§2 新增 2+1' 连接档）：node-1/2 用 80 万 fd 档、node-3 用 50 万档；conntrack 两档都必须关（30 万连接 ≫ 默认 65536 表项，不关=静默丢包）；S3 混部阶段可以不回退（参数是上限不是行为改变）；
3. [ ] 压测机侧（三台 worker + Mac）：ip_local_port_range 扩 + tcp_tw_reuse（kernel-tuning §3）；
4. [ ] 2+1' 切换演练（units 已备）：node-3 切 brave-cs-node3（MemoryMax=5G）+ 中间件换单点
   env（RF=1 重建 topic）；node-1/2 `systemctl stop docker`；/etc/brave/env 的中间件地址改单点
   后 render-config.sh 重渲染 + restart；
5. [ ] 账号预注册：每台 seed 段批量注册（stress 工具幂等，重复执行跳过已注册）；
6. [ ] 采集目录 `mkdir -p docs/stress-results/<date>-<phase>/`；
7. [ ] S0 冒烟通过（真机重复一遍本地冒烟参数）。

## 5. 数据记录规范

```
docs/stress-results/
  2026-XX-XX-<phase>/
    plan.md            # 本轮参数（命令行原样）+ 拓扑快照（哪台跑什么）
    node1/collect.csv  # collect.sh 产出（每节点一份）
    node2/...
    n1-hold/summary.csv # stress 周期快照
    n1-hold/report.json # 结束汇总（分位数/对账/计数）
    kafka-lag.txt      # 阶段末 consumer-groups 快照
    findings.md        # 人工记录：拐点、异常、现象、结论
```

**findings.md 必填字段**（每个阶梯）：时间、参数、达成值、红线触发项、连接-内存斜率、延迟分位数表、与上一档对比结论。**没有记录的档位视为未发生**。

### 已完成的冒烟记录（本地 compose，2026-09-06）

- 环境：本地单副本栈（与本计划三节点拓扑不同，数字不可外推，仅验证工具链）；
- S0：hold 100 连接 3min + storm-echo 10 连接 100msg/s 2min；
- 结果见 `docs/stress-results/2026-09-06-S0-local/`（投递成功率、分位数、CSV 样例）。

## 6. 产出与宣称口径（面试叙事）

| 层级 | 可宣称 | 不可宣称 |
|---|---|---|
| 连接轴（2+1' 实测） | "2+1' 加权拓扑实测 N 万并发长连接（含每连接内存 XKB 斜率）" | 不能称"生产拓扑承载 N 万"（2+1' 为容量实验拓扑，牺牲 HA） |
| 消息轴（混部实测） | "混部拓扑实测落库 T TPS → DAU Z 万（×10/×20 口径注明）" | 不能混用两拓扑数字；不能称"零丢失"除非实测 0 且注明窗口 |
| 单节点基准 | 独占 cs 的每连接成本与外推容量（注明"外推"） | 不能把外推值当实测值 |
| 语义 | at-least-once + 实测重复率 / 丢失率 | — |
| 方法 | 双拓扑按容量轴分治 + 阶梯加压 + 红线停止 + 预测→实测→校准闭环 | — |
