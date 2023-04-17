// Package registry provides a redis registry for podchaos.
// Reference: https://github.com/hertz-contrib/registry/blob/e0024eacdbb74cb5e334f2d060dad90324ef29d6/redis/registry.go
package registry

import (
	"context"
	"net"
	"sync"

	"github.com/redis/go-redis/v9"
)

type Info struct {
	// ServiceName will be set in podchaos by default
	ServiceName string
	// Addr will be set in podchaos by default
	Addr net.Addr
	// Weight will be set in podchaos by default
	Weight int
	// extend other infos with Tags.
	Tags map[string]string
}

type Registry struct {
	client *redis.Client
	rctx   *registryContext
	mu     sync.Mutex
	wg     sync.WaitGroup
}

type registryContext struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func NewRegistry(addr string, opts ...Option) *Registry {
	redisOpts := &redis.Options{
		Addr:     addr,
		Password: "",
		DB:       0,
	}
	for _, opt := range opts {
		opt(redisOpts)
	}
	rdb := redis.NewClient(redisOpts)
	return &Registry{
		client: rdb,
	}
}

func (r *Registry) Register(info *Info) error {
	if err := validateRegistryInfo(info); err != nil {
		return err
	}
	rctx := registryContext{}
	rctx.ctx, rctx.cancel = context.WithCancel(context.Background())
	m := newMentor()
	r.wg.Add(1)
	go m.subscribe(rctx.ctx, info, r)
	r.wg.Wait()
	rdb := r.client
	hash, err := prepareRegistryHash(info)
	if err != nil {
		return err
	}
	r.mu.Lock()
	r.rctx = &rctx
	rdb.HSet(rctx.ctx, hash.key, hash.field, hash.value)
	rdb.Expire(rctx.ctx, hash.key, defaultExpireTime)
	rdb.Publish(rctx.ctx, hash.key, generateMsg(register, info.ServiceName, info.Addr.String()))
	r.mu.Unlock()
	go m.monitorTTL(rctx.ctx, hash, info, r)
	go keepAlive(rctx.ctx, hash, r)
	return nil
}

func (r *Registry) Deregister(info *Info) error {
	if err := validateRegistryInfo(info); err != nil {
		return err
	}
	rctx := r.rctx
	rdb := r.client
	hash, err := prepareRegistryHash(info)
	if err != nil {
		return err
	}
	r.mu.Lock()
	rdb.HDel(rctx.ctx, hash.key, hash.field)
	rdb.Publish(rctx.ctx, hash.key, generateMsg(deregister, info.ServiceName, info.Addr.String()))
	rctx.cancel()
	r.mu.Unlock()
	return nil
}
