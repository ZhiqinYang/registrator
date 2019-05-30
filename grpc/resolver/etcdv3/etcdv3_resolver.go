// Package dns implements a etcdv3 resolver to be installed as the default resolver
// in grpc.
package etcdv3

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"go.etcd.io/etcd/clientv3"
	"google.golang.org/grpc/resolver"
)

func init() {
	resolver.Register(NewBuilder())
}

// NewBuilder creates a etcdv3Builder which is used to factory ETCDV3 resolvers.
func NewBuilder() resolver.Builder {
	return &etcdv3Builder{}
}

type etcdv3Builder struct{}

func NewDSN(srv, addr, backend string) string {
	return fmt.Sprintf("etcdv3://%s/%s/%s", srv, addr, backend)
}

func parseTarget(endpoint string) (string, string, error) {
	x := strings.Split(endpoint, "/")
	if len(x) != 2 {
		return "", "", fmt.Errorf("target : parse target[%s] error", endpoint)
	}
	return x[0], x[1], nil
}

// Scheme returns the naming scheme of this resolver builder, which is "dns".
func (b *etcdv3Builder) Scheme() string {
	return "etcdv3"
}

// Build creates and starts a ETCDV3 resolver that watches the name resolution of the target.
func (b *etcdv3Builder) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOption) (resolver.Resolver, error) {
	addr, backend, err := parseTarget(target.Endpoint)
	if err != nil {
		return nil, err
	}
	r := &etcdv3Resolver{}
	r.addresses = make(map[string]resolver.Address)
	r.cc = cc
	r.rn = make(chan struct{})
	r.prefix = fmt.Sprintf("/%s/%s", backend, target.Authority)
	r.t = time.NewTicker(30 * time.Second)
	r.ctx, r.cancel = context.WithCancel(context.Background())
	r.client, err = clientv3.New(clientv3.Config{
		Endpoints:   []string{addr},
		DialTimeout: 30 * time.Second,
	})

	if err != nil {
		log.Println("init err", err.Error())
	}

	go r.watcher()
	r.rn <- struct{}{}
	return r, err
}

// etcdv3Resolver watches for the name resolution update for a non-IP target.
type etcdv3Resolver struct {
	addresses map[string]resolver.Address
	client    *clientv3.Client
	cc        resolver.ClientConn
	rn        chan struct{}
	prefix    string
	wg        sync.WaitGroup
	t         *time.Ticker
	ctx       context.Context
	cancel    context.CancelFunc
	change    bool
}

// ResolveNow invoke an immediate resolution of the target that this etcdv3Resolver watches.
func (e *etcdv3Resolver) ResolveNow(opt resolver.ResolveNowOption) {
	select {
	case e.rn <- struct{}{}:
	default:
	}
}

// Close closes the etcdv3Resolver.
func (e *etcdv3Resolver) Close() {
	e.cancel()
	e.wg.Wait()
	e.client.Close()
}

func (e *etcdv3Resolver) updateState() {
	if !e.change {
		return
	}
	e.change = false
	// state := resolver.State{}
	var addr []resolver.Address
	for _, v := range e.addresses {
		//	state.Addresses = append(state.Addresses, v)
		addr = append(addr, v)
	}

	log.Println(addr)
	e.cc.NewAddress(addr)
}

func unmarshal(bts []byte) (resolver.Address, error) {
	addr := resolver.Address{}
	err := json.Unmarshal(bts, &addr)
	addr.Metadata = nil
	return addr, err
}

func (e *etcdv3Resolver) watcher() {
	e.wg.Add(1)
	defer e.wg.Done()
	rch := e.client.Watch(e.ctx, e.prefix, clientv3.WithPrefix())
	for {
		select {
		case <-e.ctx.Done():
			return
		case wresp := <-rch:
			for _, ev := range wresp.Events {
				switch ev.Type {
				case clientv3.EventTypePut:
					addr, err := unmarshal(ev.Kv.Value)
					if err != nil {
						log.Println("etcd: failed to unmarshal service:", err)
						continue
					}
					e.addresses[string(ev.Kv.Key)] = addr
					log.Println("etcd: event with put:", string(ev.Kv.Key))
				case clientv3.EventTypeDelete:
					delete(e.addresses, string(ev.Kv.Key))
					log.Println("etcd: event with delete:", string(ev.Kv.Key))
				}
				e.change = true
			}
			e.updateState()
		case <-e.t.C:
			if err := e.client.Sync(context.TODO()); err != nil {
				log.Println("etcd : sync error -->", err.Error())
			}
		case <-e.rn:
			log.Println("etcd : star sync service", e.prefix)
			// sync all service
			resp, err := e.client.Get(context.TODO(), e.prefix, clientv3.WithPrefix())
			if err != nil {
				log.Println("etcd: failed to deregister service:", err)
				continue
			}

			for _, kv := range resp.Kvs {
				addr, err := unmarshal(kv.Value)
				if err != nil {
					log.Println("etcd: failed to unmarshal service:", err)
					continue
				}
				e.addresses[string(kv.Key)] = addr
				e.change = true
			}
			e.updateState()
		}
	}
}
