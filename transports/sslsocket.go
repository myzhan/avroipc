package transports

import (
	"crypto/tls"
	"fmt"
	"net"
)

type sslsocket struct {
	net.Conn
}

var _ Transport = new(sslsocket)

func NewSSLSocket(hostPort string, tlsConfig *tls.Config) (Transport, error) {
	addr, err := net.ResolveTCPAddr("tcp", hostPort)
	if err != nil {
		return nil, err
	}

	s := &sslsocket{}
	s.Conn, err = tls.Dial(addr.Network(), addr.String(), tlsConfig)
	if err != nil {
		return nil, fmt.Errorf("TLS connection failed: %w", err)
	}

	return s, nil
}

func (s *sslsocket) Flush() error {
	return nil
}
