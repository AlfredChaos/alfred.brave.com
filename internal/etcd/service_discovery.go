package etcd

import (
	"context"
	"sync"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type ServiceDiscovery struct {
	client     *clientv3.Client
	serverList map[string]string
	lock       sync.Mutex
}

func NewServiceDiscovery(client *clientv3.Client) *ServiceDiscovery {
	return &ServiceDiscovery{
		client:     client,
		serverList: make(map[string]string),
	}
}

// WatchService 订阅某类服务（kind 为 "" 时订阅全部，仅调试用）。
// key 结构 services/{kind}/{uuid}，watch 前缀 services/{kind}/。
func (s *ServiceDiscovery) WatchService(kind string) error {
	prefix := generateNamespace(PrefixService, kind) + "/"
	resp, err := s.client.Get(context.Background(), prefix, clientv3.WithPrefix())
	if err != nil {
		log.Errorf("service_discory get with prefix %s error, %v", prefix, err)
		return err
	}
	for _, ev := range resp.Kvs {
		s.SetServiceList(string(ev.Key), string(ev.Value))
	}
	go s.watcher(prefix)
	return nil
}

// 新增服务地址
func (s *ServiceDiscovery) SetServiceList(key, value string) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.serverList[key] = string(value)
}

// 删除服务地址
func (s *ServiceDiscovery) DelServiceList(key string) {
	s.lock.Lock()
	defer s.lock.Unlock()
	delete(s.serverList, key)
}

// 获取服务地址
func (s *ServiceDiscovery) GetServices() []string {
	s.lock.Lock()
	defer s.lock.Unlock()
	addrs := make([]string, 0)

	for _, v := range s.serverList {
		addrs = append(addrs, v)
	}
	return addrs
}

func (s *ServiceDiscovery) Close() error {
	return s.client.Close()
}

func (s *ServiceDiscovery) watcher(prefix string) {
	resChan := s.client.Watch(context.Background(), prefix, clientv3.WithPrefix())
	for wresp := range resChan {
		for _, ev := range wresp.Events {
			switch ev.Type {
			case mvccpb.PUT:
				s.SetServiceList(string(ev.Kv.Key), string(ev.Kv.Value))
			case mvccpb.DELETE:
				s.DelServiceList(string(ev.Kv.Key))
			}
		}
	}
}

// var endpoints = []string{"localhost:2379"}
// ser := NewServiceDiscovery(endpoints)
// defer ser.Close()
// ser.WatchService("/web/")
// ser.WatchService("/gRPC/")
// for {
// 	select {
// 	case <-time.Tick(10 * time.Second):
// 		log.Println(ser.GetServices())
// 	}
// }
