package commands

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"alfred.brave.com/common"
	"alfred.brave.com/conf"
	"encoding/json"

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

	// onDelivered → chat.ack（T07 接线）；onOffline → chat.notify（T16 接线）
	var ackProducer *ibrave.Producer
	if len(config.KafkaBrokers()) > 0 {
		ackProducer = ibrave.NewProducer(config.KafkaBrokers(), chat.TopicAck)
		defer ackProducer.Close()
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
			// T16：produce chat.notify；当前阶段离线静默（补拉兜底），日志留痕
			log.Debugf("offline push skipped: to=%s conv=%s seq=%d", push.ToUID, push.ConvID, push.Seq)
		},
	)

	go func() {
		if err := worker.Run(cctx); err != nil {
			log.Errorf("deliver worker exited: %v", err)
			cancel()
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Info("deliver: shutting down")
	cancel()
	config.Shutdown()
	return nil
}
