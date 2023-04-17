package registry

import (
	"context"
	"encoding/json"
	"net"

	"github.com/arcosx/podchaos/pkg/utils"
	"github.com/redis/go-redis/v9"
	"k8s.io/klog/v2"
)

type Resolver struct {
	client *redis.Client
}

type Result struct {
	CacheKey  string
	Instances []Instance
}

type TargetInfo struct {
	Host string
	Tags map[string]string
}

func NewRedisResolver(addr string, opts ...Option) *Resolver {
	redisOpts := &redis.Options{Addr: addr}
	for _, opt := range opts {
		opt(redisOpts)
	}
	rdb := redis.NewClient(redisOpts)
	return &Resolver{
		client: rdb,
	}
}

func (r *Resolver) Target(_ context.Context, target *TargetInfo) string {
	return target.Host
}

func (r *Resolver) Resolve(ctx context.Context, desc string) (Result, error) {
	rdb := r.client
	fvs := rdb.HGetAll(ctx, generateKey(desc, server)).Val()
	var its []Instance
	for f, v := range fvs {
		var ri registryInfo
		err := json.Unmarshal([]byte(v), &ri)
		if err != nil {
			klog.Infof("podchaos: fail to unmarshal with err: %v, ignore instance Addr: %v", err, f)
			continue
		}
		weight := ri.Weight
		if weight <= 0 {
			weight = defaultWeight
		}
		its = append(its, NewInstance(tcp, ri.Addr, weight, ri.Tags))
	}
	return Result{
		CacheKey:  desc,
		Instances: its,
	}, nil
}

func (r *Resolver) Name() string {
	return Redis
}

type Instance struct {
	addr   net.Addr
	weight int
	tags   map[string]string
}

func NewInstance(network, address string, weight int, tags map[string]string) Instance {
	return Instance{
		addr:   utils.NewNetAddr(network, address),
		weight: weight,
		tags:   tags,
	}
}

func (i *Instance) Address() net.Addr {
	return i.addr
}

func (i *Instance) Weight() int {
	if i.weight > 0 {
		return i.weight
	}
	return DefaultWeight
}

func (i *Instance) Tag(key string) (value string, exist bool) {
	value, exist = i.tags[key]
	return
}
