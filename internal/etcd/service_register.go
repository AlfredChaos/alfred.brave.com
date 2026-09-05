package etcd

import (
	"context"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// maxRegisterBackoff etcd 不可达时重注册退避上限：30s 内不放大，避免长时间
// 分区恢复后还要等一个巨大的退避值才回到服务发现。
const maxRegisterBackoff = 30 * time.Second

type ServiceRegister struct {
	client        *clientv3.Client
	leaseId       clientv3.LeaseID
	keepAliveChan <-chan *clientv3.LeaseKeepAliveResponse
	key           string
	value         string
	ttl           int64
}

// NewServiceRegister 构造注册器并立即尝试首次注册。
// 首次失败（etcd 暂不可达）也返回可用的注册器与 err：调用方不应放弃，
// 交给 Run 维持循环退避重试——joker/feed 启动时 etcd 可能尚未就绪。
func NewServiceRegister(kind, service_id, host string, lease int64, client *clientv3.Client) (*ServiceRegister, error) {
	service := &ServiceRegister{
		client: client,
		key:    ServiceKey(kind, service_id),
		value:  host,
		ttl:    lease,
	}
	err := service.putKeyWithLease(context.Background(), lease)
	return service, err
}

// 设置租约并写入注册键。ctx 贯穿 Grant/Put/KeepAlive：进程退出路径能
// 中断挂起的 etcd 调用，而不是靠 Background 泄漏 goroutine。
func (s *ServiceRegister) putKeyWithLease(ctx context.Context, lease int64) error {
	resp, err := s.client.Grant(ctx, lease)
	if err != nil {
		log.Errorf("generate lease error = %v", err)
		return err
	}
	log.Infof("generate lease %v success", resp.ID)
	_, err = s.client.Put(ctx, s.key, s.value, clientv3.WithLease(resp.ID))
	if err != nil {
		log.Errorf("set lease %v error = %v", resp.ID, err)
		return err
	}
	leaseRespChan, err := s.client.KeepAlive(ctx, resp.ID)
	if err != nil {
		log.Errorf("set lease keepalive error = %v", err)
		return err
	}
	s.leaseId = resp.ID
	s.keepAliveChan = leaseRespChan
	return nil
}

// Run 维持注册直至 ctx 取消，是服务注册的常驻循环：
//   - 正常路径：消费 keepalive 响应（续约由 etcd client 自动完成）；
//   - lease 失效：keepalive channel 关闭（etcd 重启/网络分区超过 TTL 后租约在
//     服务端过期）→ 自动重建 lease 重注册；
//   - etcd 不可达：注册失败按指数退避（1s→30s 封顶）重试，成功后重置。
//
// 2026-09-05 教训：此前 lease 失效后无人重注册，服务进程存活却永久掉出服务发现
// ——gateway 把登录全压到剩余节点、GHOST 误清该节点在线表，需重启进程才能恢复。
func (s *ServiceRegister) Run(ctx context.Context) {
	backoff := time.Second
	for {
		if ctx.Err() != nil {
			return
		}
		if s.keepAliveChan != nil {
			for leaseKeepResp := range s.keepAliveChan {
				log.Infof("lease %v renew success, result = %v", s.leaseId, leaseKeepResp)
			}
			if ctx.Err() != nil {
				return // 进程退出：ctx 取消连带关闭 keepalive channel，不是失效
			}
			log.Warnf("lease %v expired (etcd restart/partition), re-registering %s", s.leaseId, s.key)
			s.keepAliveChan = nil
		}
		if err := s.putKeyWithLease(ctx, s.ttl); err != nil {
			log.Warnf("register %s failed: %v (retry in %s)", s.key, err, backoff)
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			if backoff *= 2; backoff > maxRegisterBackoff {
				backoff = maxRegisterBackoff
			}
			continue
		}
		log.Infof("service %s = %s registered with lease %v (ttl %ds)", s.key, s.value, s.leaseId, s.ttl)
		backoff = time.Second
	}
}

// 注销服务：只撤销自己的 lease（注册键随之删除）。etcd client 的生命周期归
// conf 全局管理（多注册器共享），这里不代为 Close。
func (s *ServiceRegister) Close() error {
	if s.leaseId == 0 {
		return nil // 从未注册成功（etcd 一直不可达），无 lease 可撤
	}
	if _, err := s.client.Revoke(context.Background(), s.leaseId); err != nil {
		log.Errorf("revoke lease %v failed, error = %v", s.leaseId, err)
		return err
	}
	log.Infof("revoke lease %v success", s.leaseId)
	return nil
}
