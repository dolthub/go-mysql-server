package server

import "net"

func newNetListener(protocol, address string) (net.Listener, error) {
	return net.Listen(protocol, address)
}
