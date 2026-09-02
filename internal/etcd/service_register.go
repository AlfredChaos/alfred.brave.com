package etcd

import (
	"context"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type ServiceRegister struct {
	client        *clientv3.Client
	leaseId       clientv3.LeaseID
	keepAliveChan <-chan *clientv3.LeaseKeepAliveResponse
	key           string
	value         string
}

func NewServiceRegister(kind, service_id, host string, lease int64, client *clientv3.Client) (*ServiceRegister, error) {
	key := ServiceKey(kind, service_id)
	service := &ServiceRegister{
		client: client,
		key:    key,
		value:  host,
	}
	if err := service.putKeyWithLease(lease); err != nil {
		return nil, err
	}
	return service, nil
}

// 设置租约
func (s *ServiceRegister) putKeyWithLease(lease int64) error {
	resp, err := s.client.Grant(context.Background(), lease)
	if err != nil {
		log.Errorf("generate lease error = %v", err)
		return err
	}
	log.Infof("generate lease %v success", resp.ID)
	_, err = s.client.Put(context.Background(), s.key, s.value, clientv3.WithLease(resp.ID))
	if err != nil {
		log.Errorf("set lease %v error = %v", resp.ID, err)
		return err
	}
	leaseRespChan, err := s.client.KeepAlive(context.Background(), resp.ID)
	if err != nil {
		log.Errorf("set lease keepalive error = %v", err)
		return err
	}
	s.leaseId = resp.ID
	s.keepAliveChan = leaseRespChan
	return nil
}

// 监听续约情况
func (s *ServiceRegister) ListenLeaseRespChan() {
	defer func() { log.Debugf("lease %v close", s.leaseId) }()
	for leaseKeepResp := range s.keepAliveChan {
		log.Infof("lease %v renew success, result = %v", s.leaseId, leaseKeepResp)
	}
}

// 注销服务
func (s *ServiceRegister) Close() error {
	if _, err := s.client.Revoke(context.Background(), s.leaseId); err != nil {
		log.Errorf("revoke lease %v failed, error = %v", s.leaseId, err)
		return err
	}
	log.Infof("revoke lease %v success", s.leaseId)
	return s.client.Close()
}

// func main() {
// 	var endpoints = []string{"localhost:2379"}
// 	ser, err := NewServiceRegister(endpoints, "/web/node1", "localhost:8000", 5)
// 	if err != nil {
// 		log.Fatalln(err)
// 	}
// 	//监听续租相应chan
// 	go ser.ListenLeaseRespChan()
// 	select {
// 	// case <-time.After(20 * time.Second):
// 	// 	ser.Close()
// 	}
// }
