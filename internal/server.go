package internal

import (
	"github.com/stretchr/testify/require"
	"net"
	"testing"
)

func RunServer(t *testing.T, handler func(net.Conn) error) (string, func() error) {
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	require.NoError(t, err)

	accept := func() error {
		conn, err := ln.Accept()
		if err != nil {
			return nil
		}
		defer conn.Close()

		return handler(conn)
	}

	go func() {
		for {
			require.NoError(t, accept())
		}
	}()

	return ln.Addr().String(), ln.Close
}
