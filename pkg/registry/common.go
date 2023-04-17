package registry

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	Redis      = "redis"
	register   = "register"
	deregister = "deregister"
	podchaos   = "podchaos"
	server     = "server"
	tcp        = "tcp"
)

const (
	DefaultWeight = 10
)

const (
	defaultExpireTime    = time.Second * 60
	defaultTickerTime    = time.Second * 30
	defaultKeepAliveTime = time.Second * 60
	defaultMonitorTime   = time.Second * 30
	defaultWeight        = 10
)

type Option func(opts *redis.Options)

func WithPassword(password string) Option {
	return func(opts *redis.Options) {
		opts.Password = password
	}
}

func WithDB(db int) Option {
	return func(opts *redis.Options) {
		opts.DB = db
	}
}

func WithTLSConfig(t *tls.Config) Option {
	return func(opts *redis.Options) {
		opts.TLSConfig = t
	}
}

func WithDialer(dialer func(ctx context.Context, network, addr string) (net.Conn, error)) Option {
	return func(opts *redis.Options) {
		opts.Dialer = dialer
	}
}

func WithReadTimeout(t time.Duration) Option {
	return func(opts *redis.Options) {
		opts.ReadTimeout = t
	}
}

type registryHash struct {
	key   string
	field string
	value string
}

type registryInfo struct {
	ServiceName string            `json:"service_name"`
	Addr        string            `json:"addr"`
	Weight      int               `json:"weight"`
	Tags        map[string]string `json:"tags"`
}

func validateRegistryInfo(info *Info) error {
	if info == nil {
		return fmt.Errorf("registry.Info can not be empty")
	}
	if info.ServiceName == "" {
		return fmt.Errorf("registry.Info ServiceName can not be empty")
	}
	if info.Addr == nil {
		return fmt.Errorf("registry.Info Addr can not be empty")
	}
	return nil
}

func generateKey(serviceName, serviceType string) string {
	return fmt.Sprintf("/%s/%s/%s", podchaos, serviceName, serviceType)
}

func generateMsg(msgType, serviceName, serviceAddr string) string {
	return fmt.Sprintf("%s-%s-%s", msgType, serviceName, serviceAddr)
}

func prepareRegistryHash(info *Info) (*registryHash, error) {
	meta, err := json.Marshal(convertInfo(info))
	if err != nil {
		return nil, err
	}
	return &registryHash{
		key:   generateKey(info.ServiceName, server),
		field: info.Addr.String(),
		value: string(meta),
	}, nil
}

func convertInfo(info *Info) *registryInfo {
	return &registryInfo{
		ServiceName: info.ServiceName,
		Addr:        info.Addr.String(),
		Weight:      info.Weight,
		Tags:        info.Tags,
	}
}

func keepAlive(ctx context.Context, hash *registryHash, r *Registry) {
	ticker := time.NewTicker(defaultTickerTime)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			r.client.Expire(ctx, hash.key, defaultKeepAliveTime)
		case <-ctx.Done():
			break
		}
	}
}
