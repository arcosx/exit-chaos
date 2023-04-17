// Reference: https://github.com/cloudwego/hertz/blob/fcd5e6ccf50b73f439df564c161389a9261e63aa/pkg/common/utils/netaddr.go
package utils

import "net"

var _ net.Addr = (*NetAddr)(nil)

// NetAddr implements the net.Addr interface.
type NetAddr struct {
	network string
	address string
}

// NewNetAddr creates a new NetAddr object with the network and address provided.
func NewNetAddr(network, address string) net.Addr {
	return &NetAddr{network, address}
}

// Network implements the net.Addr interface.
func (na *NetAddr) Network() string {
	return na.network
}

// String implements the net.Addr interface.
func (na *NetAddr) String() string {
	return na.address
}
