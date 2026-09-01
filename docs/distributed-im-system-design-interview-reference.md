# 分布式即时通信系统设计面试参考

> 本文根据项目资料库中的 6 张面试笔记图片整理，并结合 Alfred Brave 当前代码结构补充为可执行的设计答案。
>
> 资料日期：2026-08-08
>
> 使用边界：本文是面试准备与工程设计参考，不代表 Brave 当前已经实现了文中全部能力。涉及当前实现的内容，以“当前代码状态”章节为准。

## 1. 这组题真正考什么

图片中的标题是“从 System Design 的角度看，为什么微信消息几乎可以瞬间送达？”。

表面上问题是“消息如何从发送方到达接收方”，实际上考察的是候选人能否把一个看似简单的产品动作拆成完整的分布式系统问题：

1. **在线连接与路由**：服务端如何知道用户在线、连接在哪个节点、应该把消息推到哪条连接。
2. **低延迟链路**：为什么使用长连接和 push，如何避免数据库、队列和群聊 fanout 阻塞发送方。
3. **可靠投递**：网络断开、客户端重连、服务端重启时，消息为什么不会丢。
4. **去重与顺序**：客户端重试为什么不会产生重复消息，离线补拉与实时推送并发时如何保持顺序。
5. **扩展性与取舍**：单聊、离线、多端、群聊的设计目标不同，不能一开始把所有组件都堆上去。

面试时最重要的不是背出 WebSocket、Kafka、Redis、etcd 等名词，而是先声明保证，再解释实现：

- 低延迟不等于“写入内存就算成功”。
- ACK 不等于“对方已经读到”。
- 重试不可避免，因此端到端通常选择 **at-least-once + 幂等去重**，而不是口头承诺 exactly-once。
- 消息顺序必须说明作用域，通常是“单个会话内有序”，不是全局有序。
- 服务发现解决“节点在哪里”，在线状态还需要解决“连接是否仍然有效”。

## 2. 统一目标与语义

为了避免后续答案互相矛盾，先定义本参考采用的最小语义。

### 2.1 三阶段目标

按照图片最后一页的取舍建议，系统分三阶段演进：

| 阶段 | 目标 | 允许暂时不做的事情 |
| --- | --- | --- |
| 第一阶段 | 单聊可靠、低延迟送达 | 群聊 fanout、多端复杂同步 |
| 第二阶段 | 离线补拉、多端同步、断点续传 | 极大群聊、复杂读状态 |
| 第三阶段 | 群聊 fanout、热点群治理、水平扩展 | 不必要的全局强一致 |

Brave 当前应优先完成第一阶段，再实现第二阶段，最后讨论第三阶段。这样每一步都有可验证的闭环。

### 2.2 客户端可见的状态

建议将“发送成功”拆成多个状态，避免用一个布尔值掩盖分布式失败：

| 状态 | 含义 | 谁可以确认 |
| --- | --- | --- |
| pending | 客户端已生成消息并等待服务端接受 | 客户端本地 |
| accepted | 服务端通过鉴权、校验并接受该消息 | 发送方 Joker |
| persisted | 消息已写入可恢复的持久化日志或消息存储 | 消息服务 |
| delivered | 接收方某个设备已收到并确认 | 接收方客户端 |
| read | 接收方已读 | 接收方客户端 |

默认答案采用如下约束：

- 对外承诺至少达到 persisted 后才向发送方返回可靠的 accepted。
- delivered 由接收方 ACK 证明，不能由发送方节点自行推断。
- 发送方重试使用稳定的 message_id，服务端按发送方和消息 ID 做幂等。
- 消息在同一会话内由服务端分配递增 conversation_seq；不同会话之间不比较顺序。

## 3. 问题一：你点发送的那一下，消息是怎么到达对方的？

### 3.1 图片中对应的问题

图片先指出一个常见但不完整的答案：

> “服务器把消息存到数据库，然后发给对方。”

这个答案只解释了“消息保存在哪里”，没有解释：

- 服务端怎么知道对方现在在线；
- 对方连接在哪一台机器上；
- 消息如何到达那台机器；
- 数据库慢时，为什么不拖慢实时投递。

### 3.2 推荐回答

我会把链路拆成“连接建立、在线注册、消息接入、目标路由、连接投递”五步。

#### 第一步：客户端建立长连接

用户登录后，客户端与一个 Joker 接入节点建立 WebSocket 长连接。连接建立后，Joker 为该连接创建本地 session：

~~~text
session = {
  user_id,
  device_id,
  connection_id,
  node_id,
  session_epoch,
  connected_at,
  last_heartbeat_at
}
~~~

一个连接只绑定一个设备会话；一个用户可以同时拥有多个设备会话。

Joker 内部至少要保证一个连接只有一个读循环和一个写循环。读循环负责接收客户端消息和 pong，写循环负责发送业务消息、ACK 和 ping。这样可以避免多个 goroutine 并发写同一个 WebSocket。

#### 第二步：注册在线路由

连接建立后，Joker 将用户在线信息注册到 Presence/路由存储：

~~~text
presence/<user_id>/<device_id> -> {
  node_id,
  node_addr,
  connection_id,
  session_epoch
}
~~~

这个 key 必须绑定 etcd lease。客户端断开、Joker 崩溃或网络隔离后，lease 到期，其他节点才能认为该连接失效。只写一个永久 key 会产生“僵尸在线”。

本地节点同时维护一个快速查找表：

~~~text
local_sessions[user_id][device_id] -> *Session
~~~

本地表用于真正写入 WebSocket，etcd 用于跨节点查询和服务发现。不能让每一条消息都依赖一次远程 etcd 查询，否则 etcd 会进入实时消息的关键路径。

#### 第三步：发送方通过 WebSocket 发消息

客户端发送带有稳定 ID 的请求：

~~~json
{
  "cmd": "send",
  "conversation_id": "c-123",
  "client_message_id": "m-from-device-001-00042",
  "to": "user-b",
  "body": "hello"
}
~~~

Joker 完成鉴权、大小限制、会话权限和参数校验，然后把消息交给消息服务。客户端可以立即显示本地 pending 状态，但不能在没有服务端确认时显示为 delivered。

#### 第四步：查找接收方所在节点

消息服务根据 user-b 的 Presence 记录得到目标节点：

- 如果目标 session 在当前 Joker，直接查本地 session 表；
- 如果目标 session 在其他 Joker，通过节点间 RPC 转发；
- 如果没有有效 Presence，写入离线消息存储，等待接收方重连后补拉。

服务发现和用户路由是两层信息：

- 服务发现回答“有哪些 Joker 节点、地址是什么”；
- 用户 Presence 回答“这个用户当前连接在哪个节点”。

只做服务发现，不能直接知道某个用户的连接位置。

#### 第五步：写入目标连接

目标 Joker 将消息放入对应 session 的发送队列，由唯一的 WritePump 写入 WebSocket。写队列应有容量上限：

- 队列未满：写入成功，继续投递；
- 队列接近满：记录 backpressure 指标，必要时降级或断开慢连接；
- 队列满：不能无限阻塞消息路由线程，应转入可恢复的投递状态并由客户端重连补拉。

### 3.3 Brave 的当前映射

Brave 已经具备这条链路的一部分基础：

- joker/api/websocket.go 提供 /ws/:id WebSocket 接入；
- joker/exchange/client.go 的 ReadPump 和 WritePump 分离读写；
- joker/exchange/manager.go 用 sync.Map 保存本地用户连接；
- internal/etcd/scheme.go 用 User.LoginHost 和 User.JokerServiceId 记录登录位置；
- internal/etcd/service_register.go 和 service_discovery.go 提供节点注册与发现。

但当前 Client.SendMessage 的本地和异地分支仍为空，尚未形成完整的投递链路。本文的答案是目标设计，不应在面试中描述为已经实现。

## 4. 问题二：对方几乎瞬间收到，背后如何和延迟较量？

### 4.1 图片中对应的问题

核心指标是从发送方点击发送到接收方看到消息的端到端 latency。图片强调三类优化：

1. 使用长连接加 push，而不是客户端不断 polling；
2. 消息写入和消息推送解耦，避免数据库慢时拖慢实时路径；
3. 群聊 fanout 放入异步队列，不能阻塞发送方响应。

### 4.2 延迟预算

面试中不要只说“低延迟”，应该拆预算：

~~~text
client encode
  + network uplink
  + ingress processing
  + durable append
  + target routing
  + websocket write
  + client decode/render
  = perceived send-to-render latency
~~~

生产环境至少监控 p50、p95、p99，并区分：

- 客户端发送到服务端 accepted；
- accepted 到持久化完成；
- 持久化到目标节点收到；
- 目标节点收到到客户端渲染；
- 在线投递和离线补拉两类路径。

### 4.3 为什么长连接加 push

Polling 的问题是客户端必须周期性请求，即使没有消息也会产生请求和连接建立开销；轮询间隔太长会增加延迟，间隔太短会浪费电量、带宽和服务端资源。

WebSocket 长连接的优势是：

- 连接建立后复用 TCP/TLS；
- 服务端有消息即可主动写入；
- 可以复用同一条连接承载消息、ACK、心跳和同步控制帧；
- 延迟主要由网络和服务端处理决定，而不是轮询间隔决定。

长连接不是免费的。需要处理连接数、文件描述符、内存、负载均衡、连接迁移、心跳风暴和慢客户端，因此它必须配合节点级连接管理与水平扩展。

### 4.4 写入与推送如何解耦

推荐把“可靠性边界”和“实时投递”并行化：

~~~text
                ┌──► durable message log/store ──► persisted ACK
send request ───┤
                └──► delivery router ──► target Joker ──► WebSocket
~~~

更准确的做法是：

1. 服务端校验并分配 message_id、conversation_seq；
2. 将消息追加到复制的持久化日志或可靠消息存储；
3. 持久化成功后向发送方返回 accepted/persisted；
4. 同时触发在线投递；
5. 接收方 ACK 后更新 delivery 状态。

这里的“并行”不是为了绕过持久化，而是避免“先写数据库、数据库事务提交后再逐级同步处理”形成一条过长的串行链路。若业务明确允许“先展示、后落盘”，也必须把消息标为 provisional，并设计崩溃恢复和客户端纠错，不能把内存队列当作可靠存储。

### 4.5 群聊 fanout 为什么异步

假设一个群有十万成员。如果发送方请求线程同步查成员、逐个查在线节点并逐个写连接，发送方延迟会随群规模增长，并且一个热点群会拖垮消息服务。

推荐流程：

1. 发送方消息先按会话写入一次；
2. 生成一个 fanout 任务，包含 conversation_id、message_id 和成员版本；
3. worker 从成员快照生成每个设备的投递任务；
4. 在线设备走实时 push；
5. 离线设备只保留一份可按 cursor 查询的消息，不必立即创建百万条长生命周期任务；
6. 失败任务按退避策略重试，超过阈值进入死信或待人工处理队列。

群聊的关键取舍是：消息存储尽量按会话写一次，投递状态按设备或用户异步维护。对于小群可以适度 fanout-on-write；对于超大群更适合 fanout-on-read 或混合策略。

## 5. 问题三：消息不会丢，也不会重复，如何做到？

### 5.1 图片中对应的问题

图片列出的追问包括：

- 客户端如何知道消息发送成功？
- 没收到 ACK 要不要重发？
- 重发会不会造成重复消息？
- 对方离线时消息存在哪里？
- 对方重新上线后从哪里补拉？
- 重复、补拉同时发生时，消息顺序如何保证？

答案不能只说“加 ACK”。ACK 只解决一小段链路的确认，不能自动解决崩溃窗口、重试、顺序和离线同步。

### 5.2 ACK 的边界

至少区分四种确认：

1. **服务端接受 ACK**：Joker 已校验并接受请求；
2. **持久化 ACK**：消息已经写入可恢复存储；
3. **接收 ACK**：某个接收设备已收到并处理；
4. **已读 ACK**：用户已经在客户端看到或打开消息。

如果发送方在消息只存在于内存时就收到“发送成功”，而 Joker 随后崩溃，用户会看到消息已发送但永远消失。因此面试回答中应明确：可靠发送的确认点至少是复制日志或持久化存储提交之后。

### 5.3 客户端重试与幂等

网络故障时存在一个经典不确定窗口：

1. 服务端已经持久化并投递消息；
2. ACK 在返回途中丢失；
3. 客户端认为失败并重试。

服务端无法仅凭网络状态判断第一次是否成功，所以必须接受重试，并用稳定的 client_message_id 去重：

~~~text
dedup_key = (sender_id, device_id, client_message_id)
~~~

插入消息时建立唯一约束或幂等记录：

- 第一次请求：创建消息并返回 message_id；
- 重复请求：返回第一次的结果，不再次创建消息；
- 相同客户端 ID 但内容不同：视为协议错误，拒绝请求并告警。

这实现的是 at-least-once 传输配合幂等效果。网络层面不能轻易承诺 exactly-once，因为服务端和客户端之间总会存在确认丢失窗口。

### 5.4 离线消息和断点补拉

接收方不在线时，消息必须留在可靠存储中。不要把它只放在某个 Joker 进程的 channel 或内存 map 里。

客户端登录或 WebSocket 重连时携带每个会话的同步游标：

~~~json
{
  "cmd": "sync",
  "conversation_id": "c-123",
  "after_seq": 987
}
~~~

服务端按 conversation_seq > after_seq 返回消息，并带上：

- next_seq 或新的 high-water mark；
- 是否还有更多数据；
- 消息的 message_id；
- 服务端分配的会话序号。

客户端收到实时推送和补拉结果后，都按 message_id 去重，再按 conversation_seq 排序。补拉和实时推送可以并发，但客户端不能简单按到达时间展示。

### 5.5 断线重连

推荐使用指数退避并带随机抖动：

~~~text
delay = min(base * 2^attempt + random_jitter, max_delay)
~~~

重连步骤：

1. 建立新的 WebSocket；
2. 完成鉴权；
3. 使用新 session_epoch 注册 Presence；
4. 先同步断线期间的消息；
5. 恢复实时 push；
6. 对本地 pending 消息按 client_message_id 查询发送结果或重试。

为了防止旧连接在网络恢复后“复活”并删除新连接的在线状态，注销或删除 Presence 时必须带 session_epoch 做条件判断。旧 session 不能删除新 session 的记录。

### 5.6 消息顺序

“有序”需要限定范围。建议保证同一个会话内的服务端序号递增：

~~~text
conversation_id = c-123
conversation_seq = 988, 989, 990, ...
~~~

发送方的客户端时间戳不能作为全局顺序，因为设备时钟可能漂移，多个 Joker 节点也可能同时接收请求。

实现方式可以是：

- 按会话分区，由同一个分区顺序分配序号；
- 或使用数据库/日志的单调递增序列；
- 重试只复用原 message_id，不重新分配一个业务消息；
- 客户端发现序号缺口时暂停该会话的展示或显示占位，并发起补拉；
- 不要求不同会话之间有全局顺序。

如果要求强顺序，就会增加分区协调和热点成本。因此通常只保证“单会话有序、跨会话无序”。

## 6. 问题四：拉开差距的不是知道更多组件，而是会做取舍

### 6.1 图片中的判断标准

普通回答会把长连接、消息队列、数据库、缓存、推送服务全部列出来。更好的回答会说明：

- 先保证什么；
- 暂时牺牲什么；
- 为什么这样做；
- 什么时候需要切换方案。

### 6.2 关键取舍表

| 设计问题 | 推荐默认方案 | 牺牲与原因 |
| --- | --- | --- |
| 客户端通信 | WebSocket 长连接 | 需要心跳、连接治理和 FD/内存规划 |
| 在线路由 | 本地 session 表 + etcd lease Presence | etcd 不应承载每条消息的实时写入 |
| 节点转发 | Joker 间 Unary gRPC | 离散消息实现简单；大规模流式场景再评估 Stream |
| 可靠性 | 复制持久化 + at-least-once + 幂等 | 需要 dedup 表和状态管理 |
| 顺序 | 会话内 conversation_seq | 不提供全局顺序，降低协调成本 |
| 离线 | 消息存储 + cursor 补拉 | 需要保留消息和同步状态，增加存储成本 |
| 群聊 | 小群同步/轻量 fanout，大群异步 fanout | 群消息可能有短暂投递延迟 |
| 服务发现 | etcd lease + watch | 需要处理 watch 重连和旧节点清理 |
| 多设备 | 每设备独立 cursor 和 delivery 状态 | 状态量增加，但能避免一台设备影响另一台 |

### 6.3 一个可讲清楚的三步回答

面试时间有限时，可以按下面节奏回答：

**第一步：单聊核心场景**

- WebSocket 长连接；
- Joker 本地维护连接；
- etcd 维护带 lease 的在线路由；
- 同节点直接投递，跨节点 Unary gRPC；
- 消息写入可靠存储后返回持久化 ACK；
- 接收方 ACK，客户端用 message ID 去重。

**第二步：离线和多端**

- 接收方离线时消息进入消息存储；
- 每个设备维护 conversation cursor；
- 重连后先补拉，再恢复实时推送；
- 用 session epoch 防止旧连接覆盖新连接；
- delivery 和 read 状态按设备维护。

**第三步：群聊和扩展**

- 消息按会话持久化一次；
- fanout 进入异步队列；
- 小群可以 fanout-on-write，大群采用混合策略；
- 热点群限流、分片、批量投递；
- 用 p99 延迟、队列堆积、重试率和丢弃率验证扩展效果。

## 7. 推荐的 Brave 目标架构

### 7.1 组件职责

~~~text
Client
  │ WebSocket
  ▼
Load Balancer / Joker-A
  │
  ├── local session table ──► receiver connection on Joker-A
  │
  ├── Presence lookup ──► etcd lease
  │
  ├── remote receiver ──► Unary gRPC ──► Joker-B
  │                                      │
  │                                      └── local session table
  │
  └── message store / outbox / sync cursor
~~~

建议将路径分成两个平面：

- **实时平面**：WebSocket、Joker 本地队列、节点间 gRPC，目标是低延迟；
- **可靠平面**：消息存储、ACK 状态、离线补拉、幂等记录，目标是可恢复。

实时平面出现短暂故障时，可靠平面负责让客户端重连后恢复，而不是要求所有组件都同步阻塞。

### 7.2 建议的数据模型

消息表或消息日志至少包含：

~~~text
message_id
conversation_id
conversation_seq
sender_id
client_message_id
body
created_at
persisted_at
~~~

投递状态可以单独维护：

~~~text
message_id
recipient_id
device_id
delivery_state
delivered_at
read_at
last_attempt_at
~~~

在线 Presence 至少包含：

~~~text
user_id
device_id
node_id
node_addr
connection_id
session_epoch
last_seen
lease_id
~~~

### 7.3 建议的协议命令

当前 Brave 的 MessageRequest 只有 From、To 和 Message，后续应逐步增加显式命令和消息 ID：

| cmd | 作用 |
| --- | --- |
| login | 建立用户和设备 session |
| heartbeat | 应用层心跳或连接状态同步 |
| send | 发送业务消息 |
| send_ack | 服务端确认接受或持久化 |
| deliver_ack | 接收设备确认收到 |
| read_ack | 接收设备确认已读 |
| sync | 按 cursor 补拉消息 |
| logout | 注销当前 session |

路由器应按 cmd 注册 handler，不让一个函数继续增长成所有消息类型的 if/else。

## 8. 当前代码状态与落地顺序

### 8.1 已有能力

截至 2026-08-08，项目中可直接复用的基础包括：

- joker/api/websocket.go：WebSocket 升级和连接创建；
- joker/exchange/client.go：读写 pump、发送 channel、消息 JSON 解码；
- joker/exchange/manager.go：本地连接注册、注销和查找；
- internal/etcd/scheme.go：用户登录位置模型和 UserFactory；
- internal/etcd/service_register.go：Joker 服务租约注册；
- internal/etcd/service_discovery.go：服务列表初始化和 watch；
- joker/start.go：Joker 使用 advertise_host 注册服务和记录登录地址。

### 8.2 当前缺口

这些内容在面试中必须说成“设计中”或“待实现”：

1. Client.SendMessage 的本地和异地投递分支为空；
2. poccessMessage 只反序列化并调用 SendMessage，尚未按 cmd 分发；
3. Joker 间 gRPC relay 尚不存在；
4. UserFactory.Update 当前没有把在线记录绑定到 etcd lease；
5. joker/exchange/client.go 中 time.NewTicker(1) 是 1 纳秒级 ticker，不能作为生产心跳间隔；
6. 当前没有离线消息表、消息 ACK 状态、幂等记录和 conversation cursor；
7. Manager.GetClient 对不存在的 key 直接类型断言，投递路径需要改为显式判断；
8. 当前 WebSocket 注册和断开清理还没有完整的 session epoch/fencing 设计。

### 8.3 推荐实现顺序

每一步完成后都要能通过单元测试或手动验证：

1. **本地单聊**：为 Manager 增加安全的 GetClient，实现本地 Send channel 投递；
2. **协议路由**：增加 cmd、client_message_id、会话 ID 和 ACK 状态；
3. **心跳与清理**：修正 ticker，建立 ping/pong、read deadline 和注销流程；
4. **Presence lease**：登录记录使用 lease，断开按 session epoch 条件清理；
5. **跨节点 relay**：新增独立 proto 和 Unary gRPC，异地消息转发到目标 Joker；
6. **可靠存储**：写入消息表/日志，加入发送方幂等键和会话序号；
7. **离线补拉**：实现 sync cursor、断线重连、补拉与实时消息合并；
8. **群聊扩展**：增加异步 fanout、队列监控、慢消费者和热点群策略；
9. **压测验证**：记录真实连接数、CPU、内存、FD、goroutine、p95/p99 和失败率。

## 9. 面试中的常见错误

### 错误一：把“写数据库”当作完整答案

数据库只回答消息是否保存，不能回答在线路由、连接管理和低延迟投递。必须补充 Presence、长连接和目标节点转发。

### 错误二：把 HTTP polling 当成实时系统默认方案

Polling 可以作为降级或兼容方案，但不适合高频即时消息的默认路径。需要说明连接复用、心跳、断线重连和 push。

### 错误三：只说“加 ACK”

要说明 ACK 的语义和确认边界：是服务端接受、持久化、接收还是已读？没有边界的 ACK 无法指导重试。

### 错误四：声称 exactly-once

端到端 exactly-once 很难在确认丢失时成立。更可信的回答是 at-least-once、稳定消息 ID、服务端幂等和客户端去重。

### 错误五：只说“重连后从数据库拉”

还必须说明从哪个位置拉、如何避免重复、如何检测序号缺口、实时推送与补拉并发时如何合并。

### 错误六：一开始就设计百万群聊

先完成单聊可靠链路，再讨论离线、多端和热点群。设计范围越大，越要明确哪些保证暂时不提供。

## 10. 一分钟口述版本

“我会先用 WebSocket 长连接承载实时消息。用户连接到 Joker 后，本地保存 session，同时把带 lease 的 user、device 到 Joker 节点的 Presence 注册到 etcd。发送消息时，Joker 校验请求并生成稳定的 message ID；如果接收方在本机，就写入本地 session 的发送队列，如果在其他节点，就通过节点间 Unary gRPC 转发。如果没有在线 Presence，就把消息留在可靠存储里，等对方重连后按 conversation cursor 补拉。

可靠性上，我不会把 ACK 简化成一个布尔值：持久化 ACK 表示服务端可恢复，接收 ACK 表示设备收到，已读 ACK 表示用户看过。客户端重试是必然的，所以用 client message ID 做幂等，消息服务按会话分配递增序号，客户端对实时推送和离线补拉结果去重并按序展示。单聊先保证可靠和低延迟，多端同步之后再做群聊异步 fanout。这样每个组件都有明确职责，也能解释每个取舍。”

## 11. 参考来源与维护规则

- 原始参考：用户提供的 6 张分布式即时通信 System Design 面试笔记图片；
- 项目代码：joker/、internal/etcd/、conf/ 和 README.md；
- 维护规则：每当消息协议、Joker 路由、Presence、gRPC relay 或离线存储落地时，同步更新本文的“当前代码状态”和“落地顺序”；
- 诚实性规则：本文中的目标架构、建议方案和当前实现必须明确区分，未经真实测试不得写入性能数字。
