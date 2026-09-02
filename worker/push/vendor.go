package push

import (
	"context"
)

// NotifyEvent chat.notify 载荷（deliver 判定离线时产生，D12）。
// 不含消息原文（隐私 + 免登点击拉取），只带最小通知要素。
type NotifyEvent struct {
	ToUID   string `json:"to_uid"`   // 接收者
	FromUID string `json:"from_uid"` // 触发者（谁来的消息）
	ConvID  string `json:"conv_id"`  // 点击拉取定位
	Seq     int64  `json:"seq"`      // 最新 seq（badge 可用）
}

// VendorChannel 厂商推送通道抽象（APNs/FCM 的位置）。
// 本项目不接真通道（dev-task §9.4）：接口 + mock 实现 + 日志输出，标注"演示级"。
type VendorChannel interface {
	Push(ctx context.Context, deviceToken string, title, body string, badge int) error
}

// LogVendor mock 实现：把"推送"打到日志。真实通道接入 = 新增实现（APNs/FCM），
// 上层节流/合并逻辑无需改动——这正是通道抽象的目的。
type LogVendor struct{}

func (LogVendor) Push(ctx context.Context, deviceToken, title, body string, badge int) error {
	log.Infof("[vendor-mock] push to device=%s title=%q body=%q badge=%d", deviceToken, title, body, badge)
	return nil
}
