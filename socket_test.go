package avroipc_test

import (
	"fmt"
	"io"
	"net"
	"testing"

	"github.com/myzhan/avroipc"
	"github.com/stretchr/testify/require"
)

func runServer(t *testing.T) (string, func() error) {
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	require.NoError(t, err)

	accept := func() error {
		conn, err := ln.Accept()
		if err != nil {
			return nil
		}
		defer conn.Close()

		b := make([]byte, 4)
		n, err := io.ReadFull(conn, b)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		act := string(b[:n])
		switch act {
		case "ping":
			_, _ = conn.Write([]byte("pong"))
		default:
			return fmt.Errorf("unexpected action: %s", act)
		}

		return nil
	}

	go func() {
		for {
			require.NoError(t, accept())
		}
	}()

	return ln.Addr().String(), ln.Close
}

func TestSocket(t *testing.T) {
	var n int
	var b []byte

	t.Run("error", func(t *testing.T) {
		trans, err := avroipc.NewSocket("localhost:12345")
		require.NoError(t, err)

		err = trans.Open()
		require.Error(t, err)

		require.NoError(t, trans.Close())
	})

	t.Run("success", func(t *testing.T) {
		addr, clean := runServer(t)

		trans, err := avroipc.NewSocket(addr)
		require.NoError(t, err)

		err = trans.Open()
		require.NoError(t, err)

		b = []byte("ping")
		n, err = trans.Write(b)
		require.NoError(t, err)
		require.Equal(t, 4, n)

		b = make([]byte, 4)
		n, err = io.ReadFull(trans, b)
		require.NoError(t, err)
		require.Equal(t, 4, n)
		require.Equal(t, []byte("pong"), b[:n])

		require.NoError(t, clean())
		require.NoError(t, trans.Close())
	})

	// TODO Use more robust method to test timeout errors
	t.Run("timeout", func(t *testing.T) {
		addr, clean := runServer(t)

		trans, err := avroipc.NewSocketTimeout(addr, 1)
		require.NoError(t, err)

		err = trans.Open()
		require.Error(t, err)
		require.Contains(t, err.Error(), "i/o timeout")

		require.NoError(t, clean())
		require.NoError(t, trans.Close())
	})

	t.Run("not open", func(t *testing.T) {
		trans, err := avroipc.NewSocket("")
		require.NoError(t, err)

		_, err = trans.Read([]byte{})
		require.Error(t, err)
		require.Contains(t, err.Error(), "not open")

		_, err = trans.Write([]byte{})
		require.Error(t, err)
		require.Contains(t, err.Error(), "not open")

		require.NoError(t, trans.Close())
	})

	t.Run("already open", func(t *testing.T) {
		addr, clean := runServer(t)

		trans, err := avroipc.NewSocket(addr)
		require.NoError(t, err)

		require.NoError(t, trans.Open())
		require.Error(t, trans.Open())

		require.NoError(t, clean())
		require.NoError(t, trans.Close())
	})

	t.Run("close multiple times", func(t *testing.T) {
		addr, clean := runServer(t)

		trans, err := avroipc.NewSocket(addr)
		require.NoError(t, err)

		require.NoError(t, trans.Close())
		require.NoError(t, trans.Close())

		err = trans.Open()
		require.NoError(t, err)

		require.NoError(t, trans.Close())
		require.NoError(t, trans.Close())

		require.NoError(t, clean())
	})
}

func TestNewSocket(t *testing.T) {
	_, err := avroipc.NewSocket("1:2:3")
	require.Error(t, err)
}

func TestNewSocketTimeout(t *testing.T) {
	_, err := avroipc.NewSocketTimeout("1:2:3", 1)
	require.Error(t, err)
}
