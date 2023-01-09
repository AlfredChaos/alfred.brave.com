package etcd

import (
	"context"
	"encoding/json"
	"fmt"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	PrefixUsers = iota + 1
	PrefixService
)

var Schemes = map[int]string{
	PrefixUsers:   "users",
	PrefixService: "services",
}

func generateNamespace(prefix int, id string) string {
	return fmt.Sprintf("%s/%s", Schemes[prefix], id)
}

type Factory interface {
	Update() error
	Get() error
	Delete() error
}

type User struct {
	UserId    string `json:"usr_id"`
	LoginHost string `json:"login_host"`
	LoginTime string `json:"login_time"`
}

type UserFactory struct {
	Client    *clientv3.Client
	Namespace string
	User      *User
}

func (u *UserFactory) Update() error {
	bData, _ := json.Marshal(u.User)
	if _, err := u.Client.Put(context.Background(), u.Namespace, string(bData)); err != nil {
		return errorHandler(err)
	}
	return nil
}

func (u *UserFactory) Get() error {
	resp, err := u.Client.Get(context.Background(), u.Namespace)
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
	if _, err := u.Client.Delete(context.Background(), u.Namespace, clientv3.WithPrefix()); err != nil {
		return errorHandler(err)
	}
	return nil
}
