package transports_test

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/myzhan/avroipc/transports"
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

		s := bufio.NewScanner(conn)
		for s.Scan() {
			switch s.Text() {
			case "ping":
				_, _ = conn.Write([]byte("pong"))
			case "sleep":
				time.Sleep(2 * time.Second)
				_, _ = conn.Write([]byte("sleep"))
			default:
				return fmt.Errorf("unexpected action: %s", s.Text())
			}
		}
		if s.Err() != nil {
			return err
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
		trans, err := transports.NewSocket("localhost:12345")
		require.NoError(t, err)

		err = trans.Open()
		require.Error(t, err)

		require.NoError(t, trans.Close())
	})

	t.Run("flush", func(t *testing.T) {
		trans, err := transports.NewSocket("")
		require.NoError(t, err)

		err = trans.Flush()
		require.NoError(t, err)
	})

	t.Run("success", func(t *testing.T) {
		addr, clean := runServer(t)

		trans, err := transports.NewSocket(addr)
		require.NoError(t, err)

		err = trans.Open()
		require.NoError(t, err)

		b = []byte("ping\n")
		n, err = trans.Write(b)
		require.NoError(t, err)
		require.Equal(t, 5, n)

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

		trans, err := transports.NewSocketTimeout(addr, 1)
		require.NoError(t, err)

		err = trans.Open()
		require.Error(t, err)
		require.Contains(t, err.Error(), "i/o timeout")

		require.NoError(t, clean())
		require.NoError(t, trans.Close())
	})

	t.Run("not open", func(t *testing.T) {
		trans, err := transports.NewSocket("")
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

		trans, err := transports.NewSocket(addr)
		require.NoError(t, err)

		require.NoError(t, trans.Open())
		require.Error(t, trans.Open())

		require.NoError(t, clean())
		require.NoError(t, trans.Close())
	})

	t.Run("read/write timeout", func(t *testing.T) {
		addr, clean := runServer(t)

		trans, err := transports.NewSocket(addr)
		require.NoError(t, err)

		err = trans.Open()
		require.NoError(t, err)

		err = trans.SetDeadline(time.Now().Add(time.Second))
		require.NoError(t, err)

		b = []byte("sleep\n")
		n, err = trans.Write(b)
		require.NoError(t, err)
		require.Equal(t, 6, n)

		b = make([]byte, 5)
		n, err = io.ReadFull(trans, b)
		require.Error(t, err)
		require.Contains(t, err.Error(), "i/o timeout")

		require.NoError(t, clean())
		require.NoError(t, trans.Close())
	})

	t.Run("close multiple times", func(t *testing.T) {
		addr, clean := runServer(t)

		trans, err := transports.NewSocket(addr)
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
	_, err := transports.NewSocket("1:2:3")
	require.Error(t, err)
}

func TestNewSocketTimeout(t *testing.T) {
	_, err := transports.NewSocketTimeout("1:2:3", 1)
	require.Error(t, err)
}
