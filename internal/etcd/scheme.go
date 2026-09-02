package etcd

import (
	"fmt"
	"strings"

	"github.com/spf13/viper"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	PrefixService = iota + 1
)

// 服务类型（注册 key 前缀 services/{kind}/{uuid}）：发现方按 kind 订阅自己的依赖，
// 网关选 CS 与 feed 代理互不干扰。
const (
	KindCS   = "cs"
	KindFeed = "feed"
)

var etcdConn *clientv3.Client

var Schemes = map[int]string{
	PrefixService: "services",
}

// Factory etcd 键值访问的抽象（当前仅服务表；用户登录态已迁移 PG kv，见 D05/D06）。
type Factory interface {
	Update() error
	Get() error
	Delete() error
}

func RegisterEtcdConn(conn *clientv3.Client) {
	etcdConn = conn
}

// dbClient 取 etcd 连接；未注册时按配置自建（历史懒加载路径，保持现状不重构）。
func dbClient() *clientv3.Client {
	if etcdConn == nil {
		endpoints := strings.Split(viper.GetString("etcd.endpoints"), ",")
		dialTimeout := viper.GetInt64("etcd.dial_timeout")
		etcdConn = NewEtcdClient(endpoints, dialTimeout)
	}
	return etcdConn
}

func generateNamespace(prefix int, id string) string {
	return fmt.Sprintf("%s/%s", Schemes[prefix], id)
}

// ServiceKey 服务注册 key：services/{kind}/{uuid}。
func ServiceKey(kind, serviceID string) string {
	return generateNamespace(PrefixService, kind) + "/" + serviceID
}
