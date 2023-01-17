package transports_test

import (
	"bytes"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/myzhan/avroipc/transports"
)

var (
	data           = []byte{0x78, 0x01, 0x00, 0x04, 0x00, 0xfb, 0xff, 0x74, 0x65, 0x73, 0x74, 0x01, 0x00, 0x00, 0xff, 0xff, 0x04, 0x5d, 0x01, 0xc1}
	dataShortWrite = []byte{0x78, 0x1, 0x0, 0x4, 0x0, 0xfb, 0xff, 0x74, 0x65, 0x73, 0x74, 0x0, 0x0, 0x0, 0xff, 0xff}
	flushed        = []byte{0x78, 0x01, 0x00, 0x04, 0x00, 0xfb, 0xff, 0x74, 0x65, 0x73, 0x74, 0x00, 0x00, 0x00, 0xff, 0xff, 0x01, 0x00, 0x00, 0xff, 0xff, 0x04, 0x5d, 0x01, 0xc1}
)

type mockTransport struct {
	bytes.Buffer
}

func (m *mockTransport) Close() error {
	return nil
}

func (m *mockTransport) Flush() error {
	return nil
}

func (m *mockTransport) SetDeadline(time.Time) error {
	return errors.New("test error")
}

func (m *mockTransport) SetReadDeadline(t time.Time) error {
	return errors.New("test error")
}

func (m *mockTransport) SetWriteDeadline(t time.Time) error {
	return errors.New("test error")
}

func (t *mockTransport) LocalAddr() net.Addr {
	return nil
}

func (t *mockTransport) RemoteAddr() net.Addr {
	return nil
}

func prepareZlibTransport(t *testing.T, data []byte) (transports.Transport, *mockTransport) {
	m := &mockTransport{}
	m.Buffer.Write(data)

	trans, err := transports.NewZlib(m, 1)
	require.NoError(t, err)

	return trans, m
}

func TestZlibTransport_Read(t *testing.T) {
	trans, _ := prepareZlibTransport(t, data)

	b := make([]byte, 4)
	n, err := trans.Read(b)
	require.EqualError(t, err, io.EOF.Error())
	require.Equal(t, 4, n)

	require.Equal(t, "test", string(b))
}

func TestZlibTransport_Write(t *testing.T) {
	t.Run("short write", func(t *testing.T) {
		trans, m := prepareZlibTransport(t, []byte{})

		b := []byte("test")
		n, err := trans.Write(b)
		require.NoError(t, err)
		require.Equal(t, 4, n)

		require.Equal(t, dataShortWrite, m.Bytes())
	})
	t.Run("with close", func(t *testing.T) {
		trans, m := prepareZlibTransport(t, []byte{})

		b := []byte("test")
		n, err := trans.Write(b)
		require.NoError(t, err)
		require.Equal(t, 4, n)

		err = trans.Close()
		require.NoError(t, err)

		require.Equal(t, flushed, m.Bytes())
	})
	t.Run("with flush", func(t *testing.T) {
		trans, m := prepareZlibTransport(t, []byte{})

		b := []byte("test")
		n, err := trans.Write(b)
		require.NoError(t, err)
		require.Equal(t, 4, n)

		err = trans.Flush()
		require.NoError(t, err)

		require.Equal(t, flushed[:len(flushed)-9], m.Bytes())

		err = trans.Close()
		require.NoError(t, err)

		require.Equal(t, flushed, m.Bytes())
	})
	t.Run("set deadline", func(t *testing.T) {
		d := time.Now()
		trans, _ := prepareZlibTransport(t, []byte{})

		err := trans.SetDeadline(d)
		require.EqualError(t, err, "test error")
	})
}
