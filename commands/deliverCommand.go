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
