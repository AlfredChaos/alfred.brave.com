package joker

import (
	"context"
	"fmt"
	"net"
	"net/http"
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
	grpcServer := grpc.NewServer()
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
		log.Errorf("service %s register error: %v", ServiceId, err)
		return
	}
	defer server.Close()
	go server.ListenLeaseRespChan()

	<-cctx.Done()
	log.Infof("service listening exit...")
}

func setServiceLease() int64 {
	return int64(60)
}
