package server

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"alfred.brave.com/conf"
	"alfred.brave.com/event"
	"alfred.brave.com/internal/etcd"
	"alfred.brave.com/server/api"
	"github.com/gin-gonic/gin"
)

var log = event.Log

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
	registerRoutes(router, config)

	ser := &http.Server{
		Addr:    fmt.Sprintf("%s:%d", config.GetHttpHost(), config.GetHttpPort()),
		Handler: router,
	}
	log.Infof("server: listening on %s [%s]", ser.Addr, time.Since(start))
	go StartHttp(ser)
	go StartListenService(config)

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

// Start listening joker services
func StartListenService(config *conf.Config) {
	server := etcd.NewServiceDiscovery(config.EtcdClient)
	defer server.Close()
	for {
		select {
		case <-time.Tick(5 * time.Second):
			api.Services = server.GetServices()
		}
	}
}
