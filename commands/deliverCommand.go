package commands

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"alfred.brave.com/common"
	"alfred.brave.com/conf"

	"alfred.brave.com/internal/chat"
	ibrave "alfred.brave.com/internal/kafka"
	"alfred.brave.com/worker/deliver"
	"github.com/segmentio/kafka-go"
	"github.com/urfave/cli"
)

var DeliverCommand = cli.Command{
	Name:   "deliver",
	Usage:  "Runs the deliver worker (chat.push -> route kv -> grpc relay)",
	Action: deliverAction,
}

func deliverAction(ctx *cli.Context) error {
	config, err := conf.InitConfig(ctx, common.DeliverName, []int{common.MiddlewareDatabase})
	if err != nil {
		return err
	}

	cctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := ibrave.EnsureTopics(config.KafkaBrokers(), chat.Partitions); err != nil {
		log.Warnf("ensure topics failed (broker not up?): %v", err)
	}

	// onDelivered → chat.ack；onOffline → chat.notify（D12：离线判定副产品驱动通知链路）
	var ackProducer, notifyProducer *ibrave.Producer
	if len(config.KafkaBrokers()) > 0 {
		ackProducer = ibrave.NewProducer(config.KafkaBrokers(), chat.TopicAck)
		defer ackProducer.Close()
		notifyProducer = ibrave.NewProducer(config.KafkaBrokers(), chat.TopicNotify)
		defer notifyProducer.Close()
	}

	worker := deliver.New(config.Db(), config.KafkaBrokers(), "deliver",
		func(ctx context.Context, push *chat.Push) {
			if ackProducer == nil {
				return
			}
			raw, err := json.Marshal(chat.AckFromPush(push))
			if err != nil {
				log.Errorf("marshal ack: %v", err)
				return
			}
			if err := ackProducer.Write(ctx, push.FromUID, raw); err != nil {
				log.Errorf("produce chat.ack for %s failed: %v", push.MsgID, err)
			}
		},
		func(ctx context.Context, push *chat.Push) {
			if notifyProducer == nil {
				return
			}
			// 通知不含原文（隐私 + 免登拉取，D12），只带最小要素
			notify := chat.Notify{
				ToUID:   push.ToUID,
				FromUID: push.FromUID,
				ConvID:  push.ConvID,
				Seq:     push.Seq,
			}
			raw, err := json.Marshal(notify)
			if err != nil {
				log.Errorf("marshal notify: %v", err)
				return
			}
			if err := notifyProducer.Write(ctx, push.ToUID, raw); err != nil {
				log.Errorf("produce chat.notify for %s failed: %v", push.ToUID, err)
			}
		},
	)
	// S3c：送达后 ack 走批写（SetBatchDeliveredHook），批尾一次 WriteBatch 摊薄
	// chat.ack 逐条 acks=all 的跨机确认税；onDelivered 单条闭包保留为回退路径
	// （批钩子未注入时 flushDelivered 逐条调用它，测试直调 Deliver 同路径）。
	if ackProducer != nil {
		worker.SetBatchDeliveredHook(func(ctx context.Context, pushes []*chat.Push) error {
			msgs := make([]kafka.Message, 0, len(pushes))
			for _, p := range pushes {
				raw, err := json.Marshal(chat.AckFromPush(p))
				if err != nil {
					log.Errorf("marshal ack: %v", err)
					continue
				}
				msgs = append(msgs, kafka.Message{Key: []byte(p.FromUID), Value: raw})
			}
			if len(msgs) == 0 {
				return nil
			}
			if err := ackProducer.WriteBatch(ctx, msgs); err != nil {
				return fmt.Errorf("produce %s batch(%d): %w", chat.TopicAck, len(msgs), err)
			}
			return nil
		})
	}
	if notifyProducer != nil {
		// S3c：离线通知批写（flushOffline），与 ack 同款摊薄；未注入钩子时
		// flushDelivered 逐条回退 onOffline（测试直调 Deliver 同路径）。
		worker.SetBatchOfflineHook(func(ctx context.Context, pushes []*chat.Push) error {
			msgs := make([]kafka.Message, 0, len(pushes))
			for _, p := range pushes {
				notify := chat.Notify{ToUID: p.ToUID, FromUID: p.FromUID, ConvID: p.ConvID, Seq: p.Seq}
				raw, err := json.Marshal(notify)
				if err != nil {
					log.Errorf("marshal notify: %v", err)
					continue
				}
				msgs = append(msgs, kafka.Message{Key: []byte(p.ToUID), Value: raw})
			}
			if len(msgs) == 0 {
				return nil
			}
			if err := notifyProducer.WriteBatch(ctx, msgs); err != nil {
				return fmt.Errorf("produce %s batch(%d): %w", chat.TopicNotify, len(msgs), err)
			}
			return nil
		})
	}

	workerErr := make(chan error, 1)
	go func() { workerErr <- worker.Run(cctx) }()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	select {
	case err := <-workerErr:
		if err == nil {
			cancel()
			config.Shutdown()
			return nil
		}
		// worker 致命退出必须让进程非零退出（cmd/brave.go 部署契约），由
		// compose/supervisord 重启拉起；此前只 cancel 会卡在 <-quit 变僵尸，
		// restart 策略永远不触发，消费停滞无人发现
		config.Shutdown()
		return fmt.Errorf("deliver worker exited: %w", err)
	case <-quit:
		log.Info("deliver: shutting down")
		cancel()
		config.Shutdown()
		return nil
	}
}
