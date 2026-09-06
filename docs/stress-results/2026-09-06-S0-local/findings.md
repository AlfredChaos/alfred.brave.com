# S0 工具链冒烟记录（本地 compose 栈）

- 日期：2026-09-06
- 环境：本地单副本 compose 栈（Mac + Docker Desktop VM，PG/Kafka 均在 VM 内）
  —— 与三节点生产拓扑**不同**，数字不可外推，仅验证工具链正确性（stress-plan.md §3 S0）。

## 结果

| 项 | 结果 | 判定 |
|---|---|---|
| hold 100 连接 45s | connected=100, sendFailed=0, dropped=0, CSV 正常 | ✅ |
| storm 20 连接 @20msg/s × 40s | sent=800 recv=800 **lost=0 dup=0 ooo=0**；单向 P50=20ms P95=30ms；ack P50=20ms | ✅ 工具正确性 |
| storm-echo 20 连接 @100msg/s × 60s | 无 panic/无 sendFailed；但单向 P50 持续上升至 ~9s、宽限期末仍有在途 | ⚠️ 本地容量发现（见下） |

## 冒烟抓出的工具 bug（已修，均记录在 tools/stress/README.md）

1. 回显无限循环：A↔B 互相回显雪崩 → 只回显 text=="s" 原始帧；
2. gorilla 并发写 panic（心跳/风暴/回显三写者）→ per-connection 互斥写；
3. 对账污染：echo 帧复用 mid 计入 lost/dup → 对账只认原始帧；
4. 进程退出时在途消息误计 lost → 结束加 2s 宽限。

## 意外收获：本地栈容量观察（真实数据，未外推）

- storm-echo @100msg/s（原始 100 + 回显 100 ≈ 200 msg/s 入口）时，
  **投递链路单向延迟单调上升**（P50: 1.5s→5.8s），队列持续积压；
- 同期 **ack 链路 P50 仅 100ms**（ack 不经 PG 落库：deliver → chat.ack → cs 广播），
  与 persist 链路（每消息一次 PG 事务，VM 内 fsync 慢）形成鲜明对比；
- 结论：本地环境瓶颈在 persist→PG 落库，吞吐容量约几十 msg/s 量级。
  该瓶颈假设（stress-plan.md §1.2：预计 2–5k msg/s 后恶化）待真机压测校准——
  真机 NVMe + 独立 PG 的 fsync 与 VM 不可比。

## 产物清单

- `hold/summary.csv` + `hold/report.json`
- `storm-lowrate/report.json`（工具正确性基准）
- `storm-echo-fixed/report.json`（高速率本地容量观察）
