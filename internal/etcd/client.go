package etcd

import (
	"context"
	"time"

	"alfred.brave.com/event"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
)

var log = event.Log

func NewEtcdClient(endpoints []string, timeout int64) *clientv3.Client {
	log.Debugf("endpoints = %v", endpoints)
	log.Debugf("timeout = %v", timeout)
	config := clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: time.Duration(timeout * int64(time.Second)),
	}
	setEtcdClientLogger(&config)
	client, err := clientv3.New(config)
	if err != nil {
		log.Errorf("Get etcd client error: %s", err)
		return nil
	}
	return client
}

func setEtcdClientLogger(config *clientv3.Config) {}

func errorHandler(err error) error {
	switch err {
	case context.Canceled:
		log.Errorf("ctx is canceled by another routine: %v", err)
	case context.DeadlineExceeded:
		log.Errorf("ctx is attached with a deadline is exceeded: %v", err)
	case rpctypes.ErrEmptyKey:
		log.Errorf("client-side error: %v", err)
	default:
		log.Errorf("bad cluster endpoints, which are not etcd servers: %v", err)
	}
	return err
}
