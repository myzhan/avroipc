package avroipc_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/myzhan/avroipc"
	"github.com/myzhan/avroipc/mocks"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func prepareBufferedTransport() (avroipc.Transport, *mocks.MockTransport) {
	m := &mocks.MockTransport{}
	b := avroipc.NewBufferedTransport(m, 8)

	return b, m
}

func TestBufferedTransport_Open(t *testing.T) {
	t.Run("succeed", func(t *testing.T) {
		b, m := prepareBufferedTransport()

		m.On("Open").Return(nil).Once()

		err := b.Open()
		require.NoError(t, err)
		m.AssertExpectations(t)
	})

	t.Run("failed", func(t *testing.T) {
		b, m := prepareBufferedTransport()

		m.On("Open").Return(fmt.Errorf("test error")).Once()

		err := b.Open()
		require.EqualError(t, err, "test error")
		m.AssertExpectations(t)
	})
}

func TestBufferedTransport_Close(t *testing.T) {
	t.Run("succeed", func(t *testing.T) {
		b, m := prepareBufferedTransport()

		m.On("Close").Return(nil).Once()

		err := b.Close()
		require.NoError(t, err)
		m.AssertExpectations(t)
	})

	t.Run("failed", func(t *testing.T) {
		b, m := prepareBufferedTransport()

		m.On("Close").Return(fmt.Errorf("test error")).Once()

		err := b.Close()
		require.EqualError(t, err, "test error")
		m.AssertExpectations(t)
	})
}

func TestBufferedTransport_Read(t *testing.T) {
	t.Run("few bytes", func(t *testing.T) {
		d := []byte{0x1, 0x2, 0x3, 0x4}
		x := []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0}
		b, m := prepareBufferedTransport()

		m.On("Read", x).Return(4, nil).Once().Run(func(args mock.Arguments) {
			x := args[0].([]byte)
			n := copy(x, d)
			require.Equal(t, 4, n)
		})

		a := []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0}
		e := []byte{0x1, 0x2, 0x3, 0x4, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0}
		n, err := b.Read(a)
		require.NoError(t, err)
		require.Equal(t, 4, n)
		require.Equal(t, e, a)
		m.AssertExpectations(t)
	})

	t.Run("many bytes", func(t *testing.T) {
		d := []byte{0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9, 0xa, 0xb, 0xc, 0xd, 0xe, 0xf, 0xf}
		x := []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0}
		b, m := prepareBufferedTransport()

		m.On("Read", x).Return(16, nil).Once().Run(func(args mock.Arguments) {
			x := args[0].([]byte)
			n := copy(x, d)
			require.Equal(t, 16, n)
		})

		a := []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0}
		e := []byte{0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9, 0xa}
		n, err := b.Read(a)
		require.NoError(t, err)
		require.Equal(t, 10, n)
		require.Equal(t, e, a)
		m.AssertExpectations(t)
	})

	t.Run("transport error", func(t *testing.T) {
		d := []byte{0x1, 0x2, 0x3, 0x4}
		x := []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0}
		b, m := prepareBufferedTransport()

		m.On("Read", x).Return(4, fmt.Errorf("test error")).Once().Run(func(args mock.Arguments) {
			x := args[0].([]byte)
			n := copy(x, d)
			require.Equal(t, 4, n)
		})

		a := []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0}
		e := []byte{0x1, 0x2, 0x3, 0x4, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0}
		n, err := b.Read(a)
		require.NoError(t, err)
		require.Equal(t, 4, n)
		require.Equal(t, e, a)

		n, err = b.Read(a)
		require.EqualError(t, err, "test error")
		require.Equal(t, 0, n)
		require.Equal(t, e, a)
		m.AssertExpectations(t)
	})
}

func TestBufferedTransport_Write(t *testing.T) {
	t.Run("few bytes", func(t *testing.T) {
		d := []byte{0x1, 0x2, 0x3, 0x4}
		b, m := prepareBufferedTransport()

		n, err := b.Write(d)
		require.NoError(t, err)
		require.Equal(t, 4, n)
		m.AssertExpectations(t)
	})

	t.Run("many bytes", func(t *testing.T) {
		d := []byte{0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9, 0xa}
		b, m := prepareBufferedTransport()

		m.On("Write", d).Return(4, nil).Once()

		n, err := b.Write(d)
		require.NoError(t, err)
		require.Equal(t, 10, n)
		m.AssertExpectations(t)
	})

	t.Run("some times by few bytes with flush", func(t *testing.T) {
		d := []byte{0x1, 0x2, 0x3, 0x4, 0x5}
		e1 := []byte{0x1, 0x2, 0x3, 0x4, 0x5, 0x1, 0x2, 0x3}
		e2 := []byte{0x4, 0x5}
		b, m := prepareBufferedTransport()

		m.On("Write", e1).Return(8, nil).Once()
		m.On("Write", e2).Return(2, nil).Once()
		m.On("Flush").Return(nil).Once()

		for i := 0; i < 2; i++ {
			n, err := b.Write(d)
			require.NoError(t, err)
			require.Equal(t, 5, n)
		}
		err := b.Flush()
		require.NoError(t, err)
		m.AssertExpectations(t)
	})

	t.Run("transport error", func(t *testing.T) {
		d := []byte{0x1, 0x2, 0x3, 0x4, 0x5}
		e := []byte{0x1, 0x2, 0x3, 0x4, 0x5, 0x1, 0x2, 0x3}
		b, m := prepareBufferedTransport()

		m.On("Write", e).Return(10, fmt.Errorf("test error")).Once()

		n, err := b.Write(d)
		require.NoError(t, err)
		require.Equal(t, 5, n)

		n, err = b.Write(d)
		require.EqualError(t, err, "test error")
		require.Equal(t, 3, n)
		m.AssertExpectations(t)
	})
}

func TestBufferedTransport_Flush(t *testing.T) {
	t.Run("empty buffer", func(t *testing.T) {
		b, m := prepareBufferedTransport()

		m.On("Flush").Return(nil).Once()

		err := b.Flush()
		require.NoError(t, err)
		m.AssertExpectations(t)
	})

	t.Run("non-empty buffer", func(t *testing.T) {
		d := []byte{0x1, 0x2, 0x3, 0x4}
		b, m := prepareBufferedTransport()

		m.On("Write", d).Return(4, nil).Once()
		m.On("Flush").Return(nil).Once()

		n, err := b.Write(d)
		require.NoError(t, err)
		require.Equal(t, 4, n)

		err = b.Flush()
		require.NoError(t, err)
		m.AssertExpectations(t)
	})

	t.Run("buffer error", func(t *testing.T) {
		d := []byte{0x1, 0x2, 0x3, 0x4}
		b, m := prepareBufferedTransport()

		m.On("Write", d).Return(4, fmt.Errorf("test error")).Once()

		n, err := b.Write(d)
		require.NoError(t, err)
		require.Equal(t, 4, n)

		err = b.Flush()
		require.EqualError(t, err, "test error")
		m.AssertExpectations(t)
	})

	t.Run("transport error", func(t *testing.T) {
		b, m := prepareBufferedTransport()

		m.On("Flush").Return(fmt.Errorf("test error")).Once()

		err := b.Flush()
		require.EqualError(t, err, "test error")
		m.AssertExpectations(t)
	})
}

func TestBufferedTransport_SetDeadline(t *testing.T) {
	d := time.Now()
	b, m := prepareBufferedTransport()

	m.On("SetDeadline", d).Return(nil).Once()

	err := b.SetDeadline(d)
	require.NoError(t, err)
	m.AssertExpectations(t)
}
