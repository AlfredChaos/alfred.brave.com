// Package chat 定义聊天主干的消息线协议与 topic 常量（§8）。
//
// 数据流（D02/D19）：Chat Server 只 produce Msg(chat.msg)；
// persist 落库后 produce Push(chat.push)；deliver 消费 Push 投递并 produce Ack(chat.ack)。
// 本包只含线协议与 topic 元数据，不含任何业务逻辑。
package chat

// Topic 常量与分区数（§8；生产环境由部署脚本显式建 topic，分区数在此作为规范源）。
const (
	TopicMsg    = "chat.msg"    // CS → persist；key=conv_id；12P
	TopicPush   = "chat.push"   // persist → deliver；key=to_uid；12P
	TopicAck    = "chat.ack"    // deliver → CS；key=from_uid；6P
	TopicNotify = "chat.notify" // deliver → push；key=to_uid；3P
	TopicFanout = "feed.fanout" // feed-api → fanout；key=pub_uid；6P（T10 使用）
)

// Partitions 各 topic 的规范分区数。
var Partitions = map[string]int{
	TopicMsg:    12,
	TopicPush:   12,
	TopicAck:    6,
	TopicNotify: 3,
	TopicFanout: 6,
}

// 消息类型（messages.type 同值）。
const (
	TypeSingle      = "single"
	TypeGroup       = "group"
	TypeSystemEvent = "system_event"
)

// Msg chat.msg 载荷。conv_id 允许为空——首条消息由 persist 按 (from,to) 建会话。
type Msg struct {
	MsgID    string      `json:"msg_id"`     // 预写消息（群事件）非空；常规为空由 persist 派生
	CliMsgID string      `json:"cli_msg_id"` // 客户端消息 ID，ACK 匹配与幂等派生用
	ConvID   string      `json:"conv_id"`
	FromUID  string      `json:"from_uid"`
	ToUID    string      `json:"to_uid"` // 单聊收信人；群聊为空
	Type     string      `json:"type"`   // single|group|system_event
	Content  interface{} `json:"content"`
	SentAt   int64       `json:"sent_at"`
}

// Push chat.push 载荷：落库完成后的投递信封（D03 模式 A：带货直达）。
type Push struct {
	MsgID    string      `json:"msg_id"`
	CliMsgID string      `json:"cli_msg_id"`
	ConvID   string      `json:"conv_id"`
	Seq      int64       `json:"seq"`
	FromUID  string      `json:"from_uid"`
	ToUID    string      `json:"to_uid"`
	Type     string      `json:"type"`
	Content  interface{} `json:"content"`
	Mention  bool        `json:"mention"`
}

// Ack chat.ack 载荷（T07）：送达回执，推回发送者所在 CS。
type Ack struct {
	MsgID    string `json:"msg_id"`
	CliMsgID string `json:"cli_msg_id"`
	ConvID   string `json:"conv_id"`
	Seq      int64  `json:"seq"`
	FromUID  string `json:"from_uid"`
	ToUID    string `json:"to_uid"`
}

// ContentText 单聊/普通消息的内容结构。
type ContentText struct {
	Text string `json:"text"`
}
