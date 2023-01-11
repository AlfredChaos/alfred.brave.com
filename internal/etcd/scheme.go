package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/spf13/viper"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	PrefixUsers = iota + 1
	PrefixService
)

var etcdConn *clientv3.Client

var Schemes = map[int]string{
	PrefixUsers:   "users",
	PrefixService: "services",
}

type Factory interface {
	Update() error
	Get() error
	Delete() error
}

func RegisterEtcdConn(conn *clientv3.Client) {
	etcdConn = conn
}

func dbClient() *clientv3.Client {
	if etcdConn == nil {
		endpoints := strings.Split(viper.GetString("etcd.endpoints"), ",")
		dialTimeout := viper.GetInt64("etcd.dial_timeout")
		etcdConn = NewEtcdClient(endpoints, dialTimeout)
	}
	return etcdConn
}

type User struct {
	UserId         string `json:"usr_id"`
	UserToken      string `json:"user_token"`
	JokerServiceId string `json:"joken_service_id"`
	LoginHost      string `json:"login_host"`
	LoginTime      string `json:"login_time"`
}

type UserFactory struct {
	Namespace int
	User      *User
}

func (u *UserFactory) Update() error {
	bData, _ := json.Marshal(u.User)
	ns := generateNamespace(u.Namespace, u.User.UserId)
	if _, err := dbClient().Put(context.Background(), ns, string(bData)); err != nil {
		return errorHandler(err)
	}
	return nil
}

func (u *UserFactory) Get() error {
	ns := generateNamespace(u.Namespace, u.User.UserId)
	resp, err := dbClient().Get(context.Background(), ns)
	if err != nil {
		return errorHandler(err)
	}
	for _, ev := range resp.Kvs {
		json.Unmarshal(ev.Value, u.User)
		return nil
	}
	return nil
}

func (u *UserFactory) Delete() error {
	ns := generateNamespace(u.Namespace, u.User.UserId)
	if _, err := dbClient().Delete(context.Background(), ns, clientv3.WithPrefix()); err != nil {
		return errorHandler(err)
	}
	return nil
}

func generateNamespace(prefix int, id string) string {
	return fmt.Sprintf("%s/%s", Schemes[prefix], id)
}
