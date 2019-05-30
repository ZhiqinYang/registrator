package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"strconv"
	"strings"
	"time"

	"os"

	"google.golang.org/grpc/resolver"

	"github.com/gliderlabs/registrator/bridge"
	etcdv3 "go.etcd.io/etcd/clientv3"
)

var defaultDialTimeout = 30

func init() {
	if x, ok := os.LookupEnv("ETCDV3_DIAL_TIMEOUT"); ok {
		timeout, err := strconv.Atoi(x)
		if err != nil {
			log.Printf("ETCDV3_DIAL_TIMEOUT parse error %v \n", err)
			return
		}
		defaultDialTimeout = timeout
	} else {
		log.Printf("ETCDV3_DIAL_TIMEOUT is unset, use default value %vs \n", defaultDialTimeout)
	}

	bridge.Register(new(Factory), "etcdv3")
}

type Factory struct{}

func (f *Factory) New(uri *url.URL) bridge.RegistryAdapter {
	endpoints := strings.Split(uri.Host, ";")
	cli, err := etcdv3.New(etcdv3.Config{
		Endpoints:           endpoints,
		DialTimeout:         time.Duration(defaultDialTimeout) * time.Second,
		PermitWithoutStream: true,
		DialKeepAliveTime:   15 * time.Second,
	})
	if err != nil {
		log.Fatalf("etcd dial error: %v", err)
	}
	return &EtcdAdapter{client: cli, path: uri.Path}
}

type EtcdAdapter struct {
	client *etcdv3.Client
	path   string
}

func (r *EtcdAdapter) Ping() error {
	r.syncEtcdCluster()
	endpoints := r.client.Endpoints()
	for _, ep := range endpoints {
		resp, err := r.client.Status(context.Background(), ep)
		if err != nil {
			log.Printf("endpiont [%s] status error %v \n", ep, err)
			continue
		}
		log.Printf("endpoint: %s / Leader: %v\n", ep, resp.Header.MemberId == resp.Leader)
	}
	return nil
}

func (r *EtcdAdapter) syncEtcdCluster() {
	err := r.client.Sync(context.TODO())
	if err != nil {
		log.Printf("etcd: sync cluster was unsuccessful ,error : %v \n", err)
	}
}

func (r *EtcdAdapter) Register(service *bridge.Service) error {
	r.syncEtcdCluster()
	// 防止不停的触发watch
	ok, err := r.isServiceChanged(service)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}

	bts, err := marshal(service)
	if err != nil {
		log.Println("marshal service : failed to marshal service:", err)
		return err
	}
	path := r.path + "/" + service.Name + "/" + service.ID
	_, err = r.client.Put(etcdv3.WithRequireLeader(context.TODO()), path, string(bts))
	if err != nil {
		log.Println("etcd: failed to register service:", err)
	}
	return err
}

func (r *EtcdAdapter) Deregister(service *bridge.Service) error {
	r.syncEtcdCluster()
	path := r.path + "/" + service.Name + "/" + service.ID
	_, err := r.client.Delete(etcdv3.WithRequireLeader(context.TODO()), path)
	if err != nil {
		log.Println("etcd: failed to deregister service:", err)
	}
	return err
}

func (r *EtcdAdapter) isServiceChanged(service *bridge.Service) (bool, error) {
	path := r.path + "/" + service.Name + "/" + service.ID
	resp, err := r.client.Get(context.TODO(), path)
	if err != nil {
		log.Println("etcd: failed to deregister service:", err)
		return false, err
	}
	for _, v := range resp.Kvs {
		srv, _ := unmarshal(v.Value)
		if srv.IP == service.IP && srv.Port == service.Port {
			return false, nil
		}
	}
	return true, nil
}

func unmarshal(bts []byte) (*bridge.Service, error) {
	var ret = new(bridge.Service)
	addr := resolver.Address{}
	addr.Metadata = ret
	return ret, json.Unmarshal(bts, &addr)
}

func marshal(service *bridge.Service) ([]byte, error) {
	addr := resolver.Address{}
	addr.Metadata = service
	addr.Addr = fmt.Sprintf("%s:%d", service.IP, service.Port)
	addr.ServerName = service.Name
	addr.Type = resolver.GRPCLB
	return json.Marshal(addr)
}

// 异步处理
func (r *EtcdAdapter) Refresh(service *bridge.Service) error {
	return r.Register(service)
}

var defaultOpt = []etcdv3.OpOption{
	etcdv3.WithPrefix(),
	etcdv3.WithSort(etcdv3.SortByKey, etcdv3.SortAscend),
}

func (r *EtcdAdapter) Services() ([]*bridge.Service, error) {
	// 获取所有的服务数据
	resp, err := r.client.Get(context.TODO(), r.path, defaultOpt...)
	if err != nil {
		log.Println("etcd: failed to deregister service:", err)
		return nil, err
	}
	var ret []*bridge.Service
	for _, e := range resp.Kvs {
		srv, err := unmarshal(e.Value)
		if err != nil {
			log.Println("unmarshal: failed to unmarshal service:", err)
			continue
		}
		ret = append(ret, srv)
	}
	return ret, nil
}
