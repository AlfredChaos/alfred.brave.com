package etcd

import (
	"context"
	"fmt"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/namespace"
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

type User struct {
	client *clientv3.Client
}

func (u *User) UpdateUser(user_id string, attr map[string]string) error {
	nsClient := namespace.NewKV(u.client.KV, generateNamespace(PrefixUsers, user_id))
	for key, value := range attr {
		_, err := nsClient.Put(context.Background(), key, value)
		if err != nil {
			return errorHandler(err)
		}
	}
	return nil
}

func (u *User) GetUser(user_id string) (error, map[string]string) {
	result := make(map[string]string)
	ns := generateNamespace(PrefixUsers, user_id)
	resp, err := u.client.Get(context.Background(), ns)
	if err != nil {
		return errorHandler(err), nil
	}
	for _, kv := range resp.Kvs {
		result[string(kv.Key)] = string(kv.Value)
	}
	return nil, result
}

func (u *User) DeleteUser(user_id string) error {
	nsClient := namespace.NewKV(u.client.KV, Schemes[PrefixUsers])
	if _, err := nsClient.Delete(context.Background(), user_id); err != nil {
		return errorHandler(err)
	}
	return nil
}
