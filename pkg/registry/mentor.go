package registry

import (
	"context"
	"strings"
	"sync"
	"time"

	"k8s.io/klog/v2"
)

var gm *mentor

var form = make(map[string]addrs)

type addrs []string

type mentor struct {
	mform map[string]addrs
	mu    sync.Mutex
}

func newMentor() *mentor {
	if gm != nil {
		return gm
	}
	m := &mentor{mform: form}
	gm = m
	return gm
}

func (m *mentor) subscribe(ctx context.Context, info *Info, r *Registry) {
	sub := r.client.Subscribe(ctx, generateKey(info.ServiceName, server))
	defer sub.Close()
	r.wg.Done()
	select {
	case <-ctx.Done():
		return
	default:
		ch := sub.Channel()
		for msg := range ch {
			split := strings.Split(msg.Payload, "-")
			if split[0] == register {
				m.mu.Lock()
				m.insertForm(split[1], split[2])
				klog.Infof("podchaos: service info %v", m.mform)
				m.mu.Unlock()
			} else if split[0] == deregister {
				m.mu.Lock()
				m.removeAddr(split[1], split[2])
				klog.Infof("podchaos: service info %v", m.mform)
				m.mu.Unlock()
			} else {
				klog.Info("podchaos: invalid message %v", msg)
			}
		}
	}
}

func (m *mentor) monitorTTL(ctx context.Context, hash *registryHash, info *Info, r *Registry) {
	ticker := time.NewTicker(defaultMonitorTime)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if r.client.TTL(ctx, hash.key).Val() == -2 {
				m.mu.Lock()
				m.removeService(info.ServiceName)
				m.mu.Unlock()
			}
		case <-ctx.Done():
			break
		}
	}
}

func (m *mentor) insertForm(serviceName, addr string) {
	m.mform[serviceName] = append(m.mform[serviceName], addr)
}

func (m *mentor) removeService(serviceName string) {
	delete(m.mform, serviceName)
}

func (m *mentor) removeAddr(serviceName, addr string) {
	for i, v := range m.mform[serviceName] {
		if v == addr {
			m.mform[serviceName] = append(m.mform[serviceName][:i], m.mform[serviceName][i+1:]...)
		}
	}
}
