package joker

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"alfred.brave.com/conf"
	"alfred.brave.com/event"
	"alfred.brave.com/internal/etcd"
	"alfred.brave.com/joker/api"
	"alfred.brave.com/joker/exchange"
	"github.com/gin-gonic/gin"
	uuid "github.com/satori/go.uuid"
)

var log = event.Log
var ServiceId = uuid.NewV4().String()
var Manager = exchange.NewManager()

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
	api.RegisterServiceId(ServiceId)
	registerRoutes(router, config)

	ser := &http.Server{
		Addr:    fmt.Sprintf("%s:%d", config.GetHttpHost(), config.GetHttpPort()),
		Handler: router,
	}
	api.RegisterServiceHost(ser.Addr)
	log.Infof("server: listening on %s [%s]", ser.Addr, time.Since(start))
	go StartHttp(ser)
	go ServiceRegister(ctx, config)

	// Init Websockets Manager
	go Manager.Start()

	// Graceful HTTP server shutdown
	<-ctx.Done()
	log.Info("server: shutting down")
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
	host := fmt.Sprintf("%s:%d", config.GetHttpHost(), config.GetHttpPort())
	server, err := etcd.NewServiceRegister(ServiceId, host, lease, config.EtcdClient)
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
