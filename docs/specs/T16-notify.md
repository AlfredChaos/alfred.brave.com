# T16 · 离线通知骨架（chat.notify + push worker）

## 1. 目标与范围

**做**：
1. deliver 离线分支（D12 免费副产品）produce `chat.notify`（key=to_uid，3P）——载荷最小要素（to/from/conv/seq），**不含原文**。
2. `brave push` worker：消费 chat.notify → 节流（同人 60s 合并：首条立即发 + 窗口内 badge 累加 + 到期合并播报；单设备 ≤20/h 频控）→ 厂商通道。
3. 厂商通道**不接真**（§9.4）：`VendorChannel` 接口 + `LogVendor` mock（日志输出）——**演示级**，接真通道 = 新增实现零改上层。

**不做**：APNs/FCM 真实对接；夜间静默（需客户端时区，遗留）；notify.dlq（重试 3 次入死信——毒消息 log+skip+commit 与 persist 同策略，DLQ 为已声明的全局遗留项）。

## 2. 架构符合性声明

- **D12**：离线判定在 deliver 查路由那一刻，零额外查询；通知通道慢/挂不影响 IM 主链路（独立 worker 独立消费组）。
- §4 节流策略表：同人合并 60s ✅ 频控 20/h ✅ 夜间静默（不做，声明）✅ 失败退避（mock 通道恒成功，退避逻辑待接真通道时生效——诚实声明）。

## 3. 测试计划

同人合并（首条即发、窗口内只累加、到期合并播报一次）；单设备 21 条/小时内只发 20；不同用户互不合并。注入时钟驱动。

## 4. 验收记录

```
--- PASS: TestPushMergeWindow (0.00s)
--- PASS: TestPushHourlyLimit (0.00s)
--- PASS: TestPushDifferentUsersDontMerge (0.00s)
```
门禁四项全绿（9 包）✅

## 5. Review 记录

- 演示级标注贯彻：代码注释 + spec + 最终报告三处声明 ✅
- 合并逻辑红灯修复记录：首版 pending 队列在 send 后状态丢失（每条都发），改 badge 计数模型 ✅
