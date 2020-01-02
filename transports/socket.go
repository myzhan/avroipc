package transports

import (
	"net"
	"time"
)

type socket struct {
	net.Conn
}

func NewSocket(hostPort string, timeout time.Duration) (Transport, error) {
	addr, err := net.ResolveTCPAddr("tcp", hostPort)
	if err != nil {
		return nil, err
	}

	s := &socket{}
	s.Conn, err = net.DialTimeout(addr.Network(), addr.String(), timeout)
	if err != nil {
		return nil, err
	}

	return s, nil
}

func (s *socket) Flush() error {
	return nil
}
