# goroutine 阻塞分析 · 心跳死亡线取证（S1b 补充，2026-09-08）

> 目的：为"心跳反压死亡线"结论补充**直接证据**（此前为公式推断：存活 ≈ 建连速率 × 6 分钟）。
> 方法：定向复现——node-2 单机 250k@400/s 爬坡，在存活缺口（connected − estab）张开的
> 瞬间抓 cs 的 goroutine dump（:6060/debug/pprof/goroutine?debug=1），对调用栈做阻塞点聚类。
> 预期指纹：大量 goroutine 停在 `exchange.(*Client).SendResponse` 的 **chan send**，
> 以及 `WritePump` 的 **syscall.Write/chan receive**——对应根因链：
>
> ```
> WritePump 写阻塞（客户端收不动）→ Send chan(32) 满
>   → SendResponse 裸阻塞 c.Send（client.go:172，无 select/超时）
>   → ReadPump 冻结（心跳在收但 Touch 停刷、读停转）
>   → 6 分钟 SetReadDeadline 到期 → i/o timeout 静默关闭（不落日志）
> ```

## 实验配置

- 节点：node-2（43.139.51.61 / 172.16.16.13），cs=brave-cs（Send chan 32、日志 info、GOMEMLIMIT 7GiB、MemoryMax 8G）
- 负载：stress hold 250k @ 400/s、connect-workers 32、src-ips 8、心跳分片 16 路
- 取样：缺口 >1.5 万时抓 dump；另抓一份爬坡早期（健康基线）

## 结果（待填）

## 取样结果（2026-09-08 00:0x，缺口=32,584 时抓取，total=481,724）

| 簇                         | 数量                 | 停留点                                    | 判定                                 |
| -------------------------- | -------------------- | ----------------------------------------- | ------------------------------------ |
| WritePump（client.go:103） | **339,743（70.5%）** | `message, ok := <-c.Send`（chan receive） | **正常空闲**：无消息可发，等 chan    |
| ReadPump（client.go:154）  | **141,943（29.5%）** | `ReadMessage → netpoll wait`              | **正常**：等客户端帧（netpoll 挂起） |
| 其余                       | ~38                  | —                                         | 运行时/杂项                          |
| **SendResponse 阻塞**      | **0**                | —                                         | **假设签名不存在**                   |

原始文件：gr-stall.txt（文本）/ gr-stall.pb（二进制，可 `go tool pprof` 重放）。

## 结论：服务端假设被证伪（诚实修订）

1. **服务端幸存者零阻塞**：无任何 goroutine 卡在 SendResponse/写 syscall——
   "WritePump 写阻塞 → chan 满 → SendResponse 冻结 ReadPump" 的服务端链条
   **不是**大规模死亡机制（至少在幸存者中无签名；死亡者已随 deadline 退出，但
   冻结若存在应在 6 分钟窗口内可见——未见到）。
2. 死亡机制修订为**客户端侧**：服务端 ReadPump 全部健康等待入站帧、且连接死于
   6 分钟读超时（无入站帧刷新 deadline）→ **心跳根本没有到达服务端**。
   头号嫌疑=stress worker 的分片心跳在 10 万+ goroutine 调度压力下的队头阻塞：
   16 分片串行写，任一写阻塞（对端 recv 缓冲满）→ 该分片尾部连接全部饿死。
   佐证：存活 ≈ 速率×6min 公式（最后 6 分钟内建立的连接尚未经历完整分片轮）。
3. **对 S1b 主结论的影响**：50.2 万稳态数字与"建连 71 万"不受影响；"修复路径"
   修订——优先修 stress 工具心跳（每连接独立心跳 goroutine 或写超时+跳过），
   而非服务端 SendResponse（它仍是潜在风险但非本次死因）。
   面试叙事升级：**"公式假设 → dump 取证 → 证伪服务端假设 → 修订为客户端侧"**——
   这是比"一次猜中"更真实的工程排查过程。
