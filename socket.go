package avroipc

import (
	"fmt"
	"net"
	"time"
)

type socket struct {
	conn    net.Conn
	addr    net.Addr
	timeout time.Duration
}

func NewSocket(hostPort string) (Transport, error) {
	return NewSocketTimeout(hostPort, 0)
}

func NewSocketTimeout(hostPort string, timeout time.Duration) (Transport, error) {
	addr, err := net.ResolveTCPAddr("tcp", hostPort)
	if err != nil {
		return nil, err
	}
	if len(addr.Network()) == 0 {
		return nil, fmt.Errorf("bad network")
	}
	if len(addr.String()) == 0 {
		return nil, fmt.Errorf("bad address")
	}

	return &socket{addr: addr, timeout: timeout}, nil
}

// Connects the socket, creating a new socket object if necessary.
func (s *socket) Open() error {
	if s.conn != nil {
		return fmt.Errorf("already open")
	}

	conn, err := net.DialTimeout(s.addr.Network(), s.addr.String(), s.timeout)
	if err != nil {
		return err
	}

	s.conn = conn

	return nil
}

func (s *socket) Close() error {
	if s.conn == nil {
		return nil
	}

	err := s.conn.Close()
	if err != nil {
		return err
	}

	s.conn = nil

	return nil
}

func (s *socket) Read(buf []byte) (int, error) {
	if s.conn == nil {
		return 0, fmt.Errorf("not open")
	}

	return s.conn.Read(buf)
}

func (s *socket) Write(buf []byte) (int, error) {
	if s.conn == nil {
		return 0, fmt.Errorf("not open")
	}

	return s.conn.Write(buf)
}