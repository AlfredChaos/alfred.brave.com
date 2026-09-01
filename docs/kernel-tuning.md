# WS 长连接内核调优方案 · 压测-调优-压测记录闭环

> 配套文档：`target-architecture.html`（目标架构 v4）· `architecture-decisions.md`（D13 容量口径 / D14 心跳模式）
> 对齐 AGENTS.md：P1「TCP/内核调优文档化（用户实际做过手动调优，需补成可复现记录）」+ P0「压测脚本 + 真实性能数据」。
> 参考路径：link1st/gowebsocket（27KB/连接实测）与 link1st/go-stress-testing（单机 100 万连接压测实战）——本方案遵循同类开源项目的「压测 → 调优 → 再压测 → 记录」闭环。

## 0. 目标与两条扩展轴

调优前先分清两条独立的轴，参数分档完全不同：

| 轴 | 瓶颈资源 | 本项目目标（3×4C16G） | 参考上限 |
|---|---|---|---|
| **连接容量轴**（保持长连接） | 内存 + fd | 每节点 ~10 万连接（~3G 纯 Joker 内存） | gowebsocket：27KB/连接，100 万 ≈ 25.8G（需 128G 大内存机） |
| **消息吞吐轴**（收发投递） | CPU + PG/Kafka | 落库 120 TPS / 投递 600 TPS 峰值（DAU 1 万推导，见 D13） | Kafka 层余量 2 个数量级，先碰墙的是 PG |

**铁律**：所有参数分两档——练手档（4C16G 混部）与单机大量连接档（≥64G 专用机）。不要把百万档参数直接抄到 16G 机器（tcp_mem 按内存比例计算，抄错会 OOM 或丢包）。

## 1. 闭环流程总览

```
┌─ 1 基线压测 ──► 2 瓶颈定位 ──► 3 分层调优 ──► 4 复测对比 ──► 5 记录归档 ─┐
│                                                                        │
│           （未达标回到 2；达标则固化为 baseline，进入下一压力阶梯）          │
└────────────────────────────────────────────────────────────────────────┘
```

每一轮必须产出一条记录（模板见 §8），禁止"调完感觉好了"式结论。参考 gowebsocket 的数据形态：连接数 → 内存单调表（1 万/10 万/50 万/100 万），我们的记录同样要求可复现（附命令、内核版本、参数快照）。

## 2. 系统级：文件描述符

**原理**：每个 WS 连接 = 1 个 fd；默认 `ulimit -n 1024` 在压测第一分钟就撞墙（`too many open files`）。

**练手档（10 万连接/节点）**：

```bash
# /etc/security/limits.conf（root 与普通用户都要）
*  soft  nofile  262144      # 10 万连接 ×2 余量 + 程序自身 fd（PG/Kafka/日志）
*  hard  nofile  262144
*  soft  nproc   65536
*  hard  nproc   65536
```

**百万档（单机 100 万连接）**：

```bash
*  soft  nofile  1040000     # 100 万 + 4 万程序余量（gowebsocket 实战取值）
*  hard  nofile  1040000
# 系统级总量必须大于 limits.conf
sysctl fs.file-max=2097152          # /proc/sys/fs/file-max
sysctl fs.nr_open=2097152           # 单进程上限（默认 1048576 会卡住百万）
```

**验证**：`ulimit -n`；压测中 `ls /proc/$(pidof brave)/fd | wc -l` 对比连接数。

## 3. TCP 栈调优

**原理**：长连接的内核成本 = 每连接 socket 读写缓冲（tcp_rmem/wmem）+ 全局页缓存（tcp_mem，页为单位）。默认缓冲对 IM 小消息过大（浪费内存），默认 tcp_mem 上限在高连接数下触发丢包。

**练手档（16G，目标 10 万连接）**——写 `/etc/sysctl.d/99-brave-ws.conf`：

```bash
# 每连接收/发缓冲：默认值压小（IM 消息 ~1KB），最大值保留突发空间
net.ipv4.tcp_rmem = 4096 8192 262144
net.ipv4.tcp_wmem = 4096 8192 262144

# 全局 TCP 页缓存（页 = 4KB）：16G 机器取 ~2% 为下限、6% 压力线、8% 上限
# 计算：16384MB × 1024KB / 4KB = 4194304 页总量 → 取 83886 / 629145 / 838860
net.ipv4.tcp_mem = 83886 629145 838860

# 连接建立队列（压测机瞬间打连接时防 SYN 丢弃）
net.core.somaxconn = 65535
net.core.netdev_max_backlog = 262144

# 端口与复用（压测客户端机必调；服务端顺手）
net.ipv4.ip_local_port_range = 1024 65000
net.ipv4.tcp_tw_reuse = 1

# keepalive 交给应用层心跳（D14），内核 keepalive 仅作兜底拉长
net.ipv4.tcp_keepalive_time = 600
net.ipv4.tcp_keepalive_intvl = 60
net.ipv4.tcp_keepalive_probes = 3
```

**百万档（≥64G 专用机）**（对照 gowebsocket 实战值，按内存等比重算 tcp_mem）：

```bash
net.ipv4.tcp_rmem = 4096 4096 16777216
net.ipv4.tcp_wmem = 4096 4096 16777216
net.ipv4.tcp_mem  = 786432 2097152 3145728   # 128G 机器的页数配比
```

**conntrack（混部机关闭，云上注意）**：nf_conntrack 每连接一条跟踪表项，10 万连接即打满默认 65536 上限导致静默丢包。

```bash
# 检查是否加载
lsmod | grep nf_conntrack
# 方案 A：关闭（服务端专用机）
sysctl net.netfilter.nf_conntrack_max=0   # 或移除相关 iptables 规则/模块
# 方案 B：调大（必须保留防火墙时）
sysctl net.netfilter.nf_conntrack_max=1048576
```

## 4. 内存与调度

```bash
# 尽量不换出（混部机上有 Kafka pagecache，禁 swap 更稳）
vm.swappiness = 1
# 允许超额提交（百万连接预分配场景），OOM 风险由监控兜底
vm.overcommit_memory = 1
```

**Go 运行时**（应用层联动，brave 服务环境变量）：

```bash
GOMAXPROCS=4            # 混部机显式限额，避免与 Kafka/PG 抢核（容器内尤其重要）
GOGC=200                # 降低 GC 频率换内存（长连接对象存活性高，GC 收益低）
GOMEMLIMIT=3GiB         # 硬顶防 OOM（go1.19+），与 GOGC 配合
```

## 5. 应用层联动参数（与内核调优配对生效）

| 参数 | 当前 brave | 目标值 | 依据 |
|---|---|---|---|
| WritePump ticker | `time.NewTicker(1)` = 1ns（bug） | 删除主动 ping，改 D14 客户端上报模式 | 服务端省一个量级 CPU |
| 心跳超时清理 | 无 | 6 分钟定时任务扫描 | gowebsocket 同款 |
| `Send` chan 缓冲 | 1000 | **16~32** | 百万连接下 1000 缓冲是最坏 ~4GB 的内存炸弹；慢消费踢线而非堆积 |
| gorilla 读写缓冲 | 1024B×2 | 保持 | 与 tcp_rmem 匹配 |
| ReadPump 批量写 | 已有（NextWriter drain） | 保持 | WS 帧合并免费减负 |
| 优雅退出 | 直接 close | 先发 Close frame → 注销 kv → 关 conn | 配合在线状态机（§4 架构文档） |

## 6. 压测工具与基线步骤

工具：`tools/stress/`（自研，AGENTS.md P0）或 [link1st/go-stress-testing](https://github.com/link1st/go-stress-testing)（`-c 并发 -n 次数 -u ws://host:37002/v1/ws/:id`）。

**注意（来自 gowebsocket 实战的教训）**：
- 压测客户端也要调优（fd + `ip_local_port_range`）；单客户端机 ~6 万连接上限，10 万连接目标需 2 台压测机，百万需 ~16 台 2C8G
- 基线压测**关闭全员广播/事件通知**（否则压的是广播风暴不是连接容量）——记录里必须注明负载形态：空闲保持 / 心跳 / 消息收发三档分开测

**阶梯**：1 万 → 3 万 → 5 万 → 10 万连接，每档稳定 10 分钟再采数。

## 7. 监控指标（压测与生产共用）

| 指标 | 采集 | 告警线（练手档） |
|---|---|---|
| 进程 fd 数 | `process_fd`（node_exporter） | >80% ulimit |
| Joker RSS / goroutine 数 | expvar / pprof | RSS >3G；goroutine > 连接数×2.2 |
| 在线连接数 | `Manager.GetClientsLen()` 暴露 metric | 突降 >30%（可能内核丢包） |
| Kafka consumer lag | burrow / kafka exporter | >10k |
| 投递 P99 | delivery histogram | >200ms |
| retrans / drop | `netstat -s`（nstat exporter） | retrans 增速异常 |

## 8. 压测记录模板（每轮一条，存 docs/bench/）

```markdown
## bench-YYYYMMDD-NN
- 环境：node-x（4C16G，内核 6.x，参数快照 hash / git rev）
- 负载形态：[空闲保持 | 心跳 30s | 消息 X TPS]（广播已关闭）
- 工具与命令：go-stress-testing -c 50000 ... （原文粘贴）
- 结果：
  | 连接数  | RSS   | goroutine | fd    | CPU% | 投递P99 | 断线 |
  | 10k     |       |           |       |      |         |      |
  | 30k     |       |           |       |      |         |      |
  | 50k     |       |           |       |      |         |      |
- 瓶颈判断：（fd / 内存 / conntrack / PG / Kafka —— 附证据：pprof top / nstat / 日志）
- 调整项：（sysctl diff 或代码 diff）
- 结论对比基线：（+/- 连接数 / 内存每连接）
```

## 9. 安全与回滚

- 所有 sysctl 变更先在压测环境验证 ≥24h 再上生产；生产变更走变更窗口，一次只改一组参数（否则归因不可能）
- 变更前快照：`sysctl -a > sysctl.$(date +%s).txt`，回滚即 `sysctl -p sysctl.<备份>`
- `overcommit_memory=1` 与 `GOMEMLIMIT` 必须成对出现，否则 OOM killer 风险
- 禁止在承载 etcd/Kafka 的机器上激进调大 `tcp_mem`（挤占 Kafka pagecache 是混部机的隐性故障源）

## 10. 一键检查清单

```bash
ulimit -n                                    # ≥ 目标连接数×1.2
cat /proc/sys/fs/file-max                    # > limits.conf 值
sysctl net.ipv4.tcp_mem net.ipv4.tcp_rmem net.ipv4.tcp_wmem
lsmod | grep conntrack                       # 混部机应为空或 max 已调大
ss -s                                        # 当前连接概览
cat /proc/$(pidof brave)/status | grep -i vm # RSS 对照预估（连接数×~30KB）
curl -s localhost:6060/debug/pprof/goroutine?debug=1 | head   # goroutine 泄漏快查
```

---

## 修订记录

| 日期 | 版本 | 变更 |
|---|---|---|
| 2026-09-01 | v1 | 初版：两轴分档、练手/百万两档参数、闭环模板；落实 AGENTS.md P1 调优文档化 + P0 压测数据任务 |
