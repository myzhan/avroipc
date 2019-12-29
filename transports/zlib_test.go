package transports_test

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/myzhan/avroipc/transports"
)

var data = []byte{0x78, 0x01, 0x00, 0x04, 0x00, 0xfb, 0xff, 0x74, 0x65, 0x73, 0x74, 0x01, 0x00, 0x00, 0xff, 0xff, 0x04, 0x5d, 0x01, 0xc1}
var flushed = []byte{0x78, 0x01, 0x00, 0x04, 0x00, 0xfb, 0xff, 0x74, 0x65, 0x73, 0x74, 0x00, 0x00, 0x00, 0xff, 0xff, 0x01, 0x00, 0x00, 0xff, 0xff, 0x04, 0x5d, 0x01, 0xc1}

type mockTransport struct {
	bytes.Buffer
}

func (m *mockTransport) Open() error {
	return errors.New("test error")
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

func TestZlibTransport_Open(t *testing.T) {
	m := &mockTransport{}

	trans, err := transports.NewZlib(m, 1)
	require.NoError(t, err)

	err = trans.Open()
	require.EqualError(t, err, "test error")
}

func TestZlibTransport_Read(t *testing.T) {
	m := &mockTransport{}
	m.Buffer.Write(data)

	trans, err := transports.NewZlib(m, 1)
	require.NoError(t, err)

	b := make([]byte, 4)
	n, err := trans.Read(b)
	require.EqualError(t, err, io.EOF.Error())
	require.Equal(t, 4, n)

	require.Equal(t, "test", string(b))
}

func TestZlibTransport_Write(t *testing.T) {
	t.Run("short write", func(t *testing.T) {
		m := &mockTransport{}

		trans, err := transports.NewZlib(m, 1)
		require.NoError(t, err)

		b := []byte("test")
		n, err := trans.Write(b)
		require.NoError(t, err)
		require.Equal(t, 4, n)

		require.Equal(t, data[:2], m.Bytes())
	})
	t.Run("with close", func(t *testing.T) {
		m := &mockTransport{}

		trans, err := transports.NewZlib(m, 1)
		require.NoError(t, err)

		b := []byte("test")
		n, err := trans.Write(b)
		require.NoError(t, err)
		require.Equal(t, 4, n)

		err = trans.Close()
		require.NoError(t, err)

		require.Equal(t, data, m.Bytes())
	})
	t.Run("with flush", func(t *testing.T) {
		m := &mockTransport{}

		trans, err := transports.NewZlib(m, 1)
		require.NoError(t, err)

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
		m := &mockTransport{}

		trans, err := transports.NewZlib(m, 1)
		require.NoError(t, err)

		err = trans.SetDeadline(d)
		require.EqualError(t, err, "test error")
	})
}
