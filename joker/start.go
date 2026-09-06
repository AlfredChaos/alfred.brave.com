package joker

import (
	"context"
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof" // BRAVE_PPROF=1 时注册 /debug/pprof 到默认 mux
	"os"
	"time"

	"alfred.brave.com/conf"
	"alfred.brave.com/database"
	"alfred.brave.com/event"
	"alfred.brave.com/internal/chat"
	"alfred.brave.com/internal/etcd"
	ibrave "alfred.brave.com/internal/kafka"
	"alfred.brave.com/joker/exchange"
	"alfred.brave.com/joker/relay"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"github.com/segmentio/kafka-go"
)

var log = event.Log
var ServiceId = uuid.NewString()

// Start the REST API server using the configuration provided
func Start(ctx context.Context, config *conf.Config) {
	defer func() {
		if err := recover(); err != nil {
			log.Error(err)
		}
	}()

	start := time.Now()

	// Create new HTTP router engine without standard middleware
	router := gin.New()

	// Register HTTP route handlers
	exchange.RegisterServiceId(ServiceId)
	registerRoutes(router, config)
	// 压测观测（stress-plan.md §4.3）：env BRAVE_PPROF=1 时暴露 localhost:6060
	// 的 goroutine/heap dump，供 collect.sh 周期拉取；默认关闭零开销
	if os.Getenv("BRAVE_PPROF") == "1" {
		go func() {
			addr := "127.0.0.1:6060"
			log.Infof("pprof listening on %s", addr)
			if err := http.ListenAndServe(addr, nil); err != nil { // net/http/pprof 默认 mux
				log.Warnf("pprof server: %v", err)
			}
		}()
	}

	ser := &http.Server{
		Addr:    fmt.Sprintf("%s:%d", config.GetHttpHost(), config.GetHttpPort()),
		Handler: router,
	}
	// 注册到 etcd 与回传客户端用的 host 必须是对外可路由地址（advertise_host），
	// 而非监听地址 ser.Addr（监听通常是 0.0.0.0，外部无法连接）。
	// wsAddr：浏览器可直连（宿主机视角）；grpcAddr：compose 网络内 deliver 可达
	wsAddr := fmt.Sprintf("%s:%d", config.GetAdvertiseHost(), config.GetAdvertisePort())
	grpcAddr := fmt.Sprintf("%s:%d", config.GetGrpcAdvertiseHost(), config.GetGrpcPort())
	exchange.RegisterServiceHost(wsAddr)
	// D06：Joker 作为 online:{uid} 唯一写者，PG kv 经构造注入
	exchange.Controller.SetOnline(exchange.NewPgOnlineKV(database.NewKvStore(config.Db())))
	exchange.Controller.SetOnlineIdentity(wsAddr, grpcAddr)
	log.Infof("server: listening on %s [%s]", ser.Addr, time.Since(start))
	go StartHttp(ser)
	go ServiceRegister(ctx, config)

	// D02：CS 只 produce chat.msg；Kafka 未部署时保持本地模式（消息链路显式不可用）
	msgProducer := ibrave.NewMsgProducer(config.KafkaBrokers())
	exchange.Controller.SetMsgProducer(msgProducer)
	defer msgProducer.Close()

	// gRPC 投递接收端（§3 步骤 9-10）：deliver worker → RelayMessage → 本地 Send 通道
	grpcLis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", config.GetHttpHost(), config.GetGrpcPort()))
	if err != nil {
		log.Errorf("grpc listen :%d failed: %v", config.GetGrpcPort(), err)
		return
	}
	// 放宽 keepalive enforcement：deliver 是离散 unary 调用，连接大部分时间无活跃流，
	// 必须允许无流 ping（默认 PermitWithoutStream=false 会直接拒绝）；MinTime 下限
	// 需低于 client 的 20s ping 间隔（deliver.go），否则被默认 5min 判为恶意踢连接。
	grpcServer := grpc.NewServer(
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             10 * time.Second,
			PermitWithoutStream: true,
		}),
	)
	relay.NewServer(exchange.Controller).Register(grpcServer)
	go func() {
		log.Infof("grpc relay listening on :%d", config.GetGrpcPort())
		if err := grpcServer.Serve(grpcLis); err != nil {
			log.Errorf("grpc serve: %v", err)
		}
	}()

	// Init Websockets Manager
	go exchange.Controller.Start()

	// chat.ack 消费者（§3 步骤 14-15）：每实例独立消费组 = 广播语义
	ackReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: config.KafkaBrokers(),
		GroupID: "cs-ack-" + ServiceId,
		Topic:   chat.TopicAck,
	})
	go func() {
		if err := exchange.ConsumeAcks(ctx, exchange.Controller, ackReader); err != nil {
			log.Errorf("ack consumer exited: %v", err)
		}
	}()

	// Graceful HTTP server shutdown
	<-ctx.Done()
	log.Info("server: shutting down")
	grpcServer.GracefulStop()
	if err := ser.Close(); err != nil {
		log.Errorf("server: shutdown failed (%s)", err)
	}
}

// StartHttp starts the web server in http mode.
func StartHttp(s *http.Server) {
	if err := s.ListenAndServe(); err != nil {
		if err == http.ErrServerClosed {
			log.Infof("server: shutdown complete")
		} else {
			log.Errorf("server: %s", err)
		}
	}
}

func ServiceRegister(cctx context.Context, config *conf.Config) {
	lease := setServiceLease()
	// 注册进 etcd 的服务地址同样用 advertise_host，供 server 发现与调用。
	host := fmt.Sprintf("%s:%d", config.GetAdvertiseHost(), config.GetAdvertisePort())
	server, err := etcd.NewServiceRegister(etcd.KindCS, ServiceId, host, lease, config.EtcdClient)
	if err != nil {
		// 首次注册失败不放弃：etcd 可能晚于 joker 就绪，Run 维持循环会退避重试
		log.Warnf("service %s initial register failed: %v (will retry)", ServiceId, err)
	}
	defer server.Close()
	go server.Run(cctx)

	<-cctx.Done()
	log.Infof("service listening exit...")
}

func setServiceLease() int64 {
	return int64(60)
}
