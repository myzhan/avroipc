package avroipc_test

import (
	"github.com/myzhan/avroipc"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/nettest"
	"io"
	"testing"
	"time"
)

func runServer(t *testing.T) (string, func() error) {
	var n int
	var b []byte

	listener, err := nettest.NewLocalListener("tcp")
	require.NoError(t, err)

	go func() {
		conn, err := listener.Accept()
		require.NoError(t, err)
		defer conn.Close()

		b = make([]byte, 4)
		n, err = io.ReadFull(conn, b)
		require.NoError(t, err)
		require.Equal(t, []byte("ping"), b[:n])

		b = []byte("pong")
		n, err = conn.Write(b)
		require.NoError(t, err)
		require.Equal(t, 4, n)
	}()

	return listener.Addr().String(), listener.Close
}

func TestSocket(t *testing.T) {
	var n int
	var b []byte

	addr, clean := runServer(t)
	defer clean()

	t.Run("success", func(t *testing.T) {
		trans, err := avroipc.NewSocket(addr)
		require.NoError(t, err)

		err = trans.Open()
		require.NoError(t, err)
		defer trans.Close()

		b = []byte("ping")
		n, err = trans.Write(b)
		require.NoError(t, err)
		require.Equal(t, 4, n)

		b = make([]byte, 4)
		n, err = io.ReadFull(trans, b)
		require.NoError(t, err)
		require.Equal(t, 4, n)
		require.Equal(t, []byte("pong"), b[:n])
	})

	t.Run("timeout", func(t *testing.T) {
		clean()

		trans, err := avroipc.NewSocketTimeout(addr, 10*time.Second)
		require.NoError(t, err)

		err = trans.Open()
		require.Error(t, err)
		require.Contains(t, err.Error(), "i/o timeout")
	})
}
