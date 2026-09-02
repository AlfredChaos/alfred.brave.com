package commands

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"alfred.brave.com/common"
	"alfred.brave.com/conf"
	"alfred.brave.com/database"
	"alfred.brave.com/internal/etcd"
	"alfred.brave.com/worker/ghost"
	"github.com/urfave/cli"
)

var GhostCommand = cli.Command{
	Name:   "ghost",
	Usage:  "Runs the online-status reconciliation worker (60s sweep)",
	Action: ghostAction,
}

// ghostAction GHOST 对账任务入口：etcd 服务表 + PG kv online:*，
// 不对外提供服务，无需注册进服务发现。
func ghostAction(ctx *cli.Context) error {
	config, err := conf.InitConfig(ctx, common.JokerName, []int{common.MiddlewareDatabase, common.MiddlewareEtcd})
	if err != nil {
		return err
	}

	cctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 服务表视图：watch 常驻维护本地列表，对账时快照
	discovery := etcd.NewServiceDiscovery(config.EtcdClient)
	defer discovery.Close()
	if err := discovery.WatchService(etcd.KindCS); err != nil {
		return err
	}

	kv := database.NewKvStore(config.Db())
	reconciler := ghost.NewReconciler(kv, func() map[string]bool {
		alive := make(map[string]bool)
		for _, addr := range discovery.GetServices() {
			alive[addr] = true
		}
		return alive
	})

	go reconciler.Run(cctx)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Info("ghost: shutting down")
	cancel()
	config.Shutdown()
	return nil
}
