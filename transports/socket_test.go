package transports_test

import (
	"bufio"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/myzhan/avroipc/internal"
	"github.com/myzhan/avroipc/transports"
)

func handler(conn net.Conn) error {
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
	return s.Err()
}

func prepareSocket(t *testing.T) (transports.Transport, func() error) {
	addr, clean := internal.RunServer(t, handler)

	trans, err := transports.NewSocket(addr, time.Second)
	require.NoError(t, err)

	return trans, clean
}

func TestSocket(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		trans, clean := prepareSocket(t)

		err := trans.Flush()
		require.NoError(t, err)

		_, err = trans.Write([]byte("ping\n"))
		require.NoError(t, err)

		b := &internal.Buffer{}
		err = b.ReadFrom(trans)
		require.NoError(t, err)
		require.Equal(t, []byte("pong"), b.Bytes())

		require.NoError(t, clean())
		require.NoError(t, trans.Close())
	})

	// TODO Use a more robust method to test timeout errors
	t.Run("timeout", func(t *testing.T) {
		addr, clean := internal.RunServer(t, handler)

		_, err := transports.NewSocket(addr, 1)
		require.Error(t, err)
		require.Contains(t, err.Error(), "i/o timeout")

		require.NoError(t, clean())
	})

	t.Run("bad address", func(t *testing.T) {
		_, err := transports.NewSocket("1:2:3", time.Second)
		require.Error(t, err)
		require.Contains(t, err.Error(), "too many colons in address")
	})

	t.Run("connection refused", func(t *testing.T) {
		_, err := transports.NewSocket("localhost:12345", time.Second)
		require.Error(t, err)
		require.Contains(t, err.Error(), "connect: connection refused")
	})

	t.Run("read/write timeout", func(t *testing.T) {
		trans, clean := prepareSocket(t)

		err := trans.SetDeadline(time.Now().Add(time.Second))
		require.NoError(t, err)

		_, err = trans.Write([]byte("sleep\n"))
		require.NoError(t, err)

		b := &internal.Buffer{}
		err = b.ReadFrom(trans)
		require.Error(t, err)
		require.Contains(t, err.Error(), "i/o timeout")

		require.NoError(t, clean())
		require.NoError(t, trans.Close())
	})

	t.Run("close multiple times", func(t *testing.T) {
		trans, clean := prepareSocket(t)

		err := trans.Close()
		require.NoError(t, err)

		err = trans.Close()
		require.Error(t, err)
		require.Contains(t, err.Error(), "use of closed network connection")

		require.NoError(t, clean())
	})
}
