package etcd

import (
	"context"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/gliderlabs/registrator/bridge"
	etcdv3 "go.etcd.io/etcd/clientv3"
)

var adapter *EtcdAdapter // bridge.RegistryAdapter

func init() {
	cli, err := etcdv3.New(etcdv3.Config{
		Endpoints:           []string{"127.0.0.1:2379"},
		DialTimeout:         time.Duration(defaultDialTimeout) * time.Second,
		PermitWithoutStream: true,
		DialKeepAliveTime:   15 * time.Second,
	})
	if err != nil {
		log.Fatalf("etcd dial error: %v", err)
	}
	adapter = &EtcdAdapter{client: cli, path: "/test"}
}

func Test_Ping(t *testing.T) {
	adapter.Ping()
}

func Test_Register(t *testing.T) {
	for i := 0; i < 10; i++ {
		srv := new(bridge.Service)
		srv.Name = fmt.Sprintf("game-%d", i%3)
		srv.Port = 8080
		srv.IP = "192.168.1.1"
		srv.TTL = 60
		srv.ID = fmt.Sprintf("%s-%d", srv.Name, i)
		if err := adapter.Register(srv); err != nil {
			t.Fatal(err)
		}

	}

}

func Test_remove(t *testing.T) {
	path := "/test/mongo/ost=TEST:mongo3:27017"
	t.Log(adapter.client.Delete(etcdv3.WithRequireLeader(context.TODO()), path))
	Test_Services(t)
}

func Test_Deregister(t *testing.T) {
	for i := 0; i < 990; i++ {
		srv := new(bridge.Service)
		srv.Name = fmt.Sprintf("game-%d", i%3)
		srv.Port = 8080
		srv.IP = "192.168.1.1"
		srv.TTL = 60
		srv.ID = fmt.Sprintf("%s-%d", srv.Name, i)
		if err := adapter.Deregister(srv); err != nil {
			t.Fatal(err)
		}
	}
}

func Test_watch(t *testing.T) {
	cli := adapter.client
	rch := cli.Watch(context.Background(), adapter.path, etcdv3.WithPrefix())
	for wresp := range rch {
		for _, ev := range wresp.Events {
			fmt.Printf("%s %q : %q\n", ev.Type, ev.Kv.Key, ev.Kv.Value)
		}
	}
}

func Test_Services(t *testing.T) {
	srvs, err := adapter.Services()
	if err != nil {
		t.Fatal(err)
	}

	for _, s := range srvs {
		t.Logf("svc : %+v", s)
	}
}
