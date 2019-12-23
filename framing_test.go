package avroipc_test

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/myzhan/avroipc"
	"github.com/myzhan/avroipc/mocks"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func prepareFramingLayer() (avroipc.FramingLayer, *mocks.MockTransport) {
	m := &mocks.MockTransport{}
	f := avroipc.NewFramingLayer(m)

	return f, m
}

func TestFramingLayer_Read(t *testing.T) {
	t.Run("no bytes", func(t *testing.T) {
		f, m := prepareFramingLayer()

		for _, d := range [][]byte{
			// Serial
			{0x0, 0x0, 0x0, 0x0},
			// Frame count
			{0x0, 0x0, 0x0, 0x1},
			// Frame length
			{0x0, 0x0, 0x0, 0x0},
		} {
			func(data []byte) {
				m.On("Read", make([]byte, len(data))).Return(len(data), nil).Once().Run(func(args mock.Arguments) {
					copy(args[0].([]byte), data)
				})
			}(d)
		}

		e := []byte(nil)
		a, err := f.Read()
		require.NoError(t, err)
		require.Equal(t, e, a)
		m.AssertExpectations(t)
	})

	t.Run("many bytes", func(t *testing.T) {
		f, m := prepareFramingLayer()

		for _, d := range [][]byte{
			// Serial
			{0x0, 0x0, 0x0, 0x0},
			// Frame count
			{0x0, 0x0, 0x0, 0x3},
			// Frame length
			{0x0, 0x0, 0x0, 0x4},
			// Frame content
			{0x1, 0x2, 0x3, 0x4},
			// Frame length
			{0x0, 0x0, 0x0, 0x4},
			// Frame content
			{0x1, 0x2, 0x3, 0x4},
			// Frame length
			{0x0, 0x0, 0x0, 0x2},
			// Frame content
			{0x1, 0x2},
		} {
			func(data []byte) {
				m.On("Read", make([]byte, len(data))).Return(len(data), nil).Once().Run(func(args mock.Arguments) {
					copy(args[0].([]byte), data)
				})
			}(d)
		}

		e := []byte{0x1, 0x2, 0x3, 0x4, 0x1, 0x2, 0x3, 0x4, 0x1, 0x2}
		a, err := f.Read()
		require.NoError(t, err)
		require.Equal(t, e, a)
		m.AssertExpectations(t)
	})

	t.Run("bad serial", func(t *testing.T) {
		d := []byte{0x0, 0x0, 0x0, 0xa}
		f, m := prepareFramingLayer()

		m.On("Read", make([]byte, 4)).Return(4, nil).Once().Run(func(args mock.Arguments) {
			copy(args[0].([]byte), d)
		})

		a, err := f.Read()
		require.EqualError(t, err, "bad serial: 0 != 10")
		require.Nil(t, a)
		m.AssertExpectations(t)
	})

	t.Run("transport error", func(t *testing.T) {
		f, m := prepareFramingLayer()

		m.On("Read", make([]byte, 4)).Return(0, fmt.Errorf("test error")).Once()

		a, err := f.Read()
		require.EqualError(t, err, "test error")
		require.Nil(t, a)
		m.AssertExpectations(t)
	})
}

func TestFramingLayer_Write(t *testing.T) {
	t.Run("no bytes", func(t *testing.T) {
		d := []byte(nil)
		f, m := prepareFramingLayer()

		err := f.Write(d)
		require.NoError(t, err)
		m.AssertExpectations(t)
	})

	t.Run("few bytes", func(t *testing.T) {
		d := []byte{0x1, 0x2, 0x3, 0x4}
		f, m := prepareFramingLayer()

		a := bytes.Buffer{}
		m.On("Write", mock.Anything).Return(0, nil).Times(4).Run(func(args mock.Arguments) {
			_, err := a.Write(args[0].([]byte))
			require.NoError(t, err)
		})

		e := []byte{
			// Serial
			0x0, 0x0, 0x0, 0x1,
			// Frame count
			0x0, 0x0, 0x0, 0x1,
			// Frame length
			0x0, 0x0, 0x0, 0x4,
			// Frame content
			0x1, 0x2, 0x3, 0x4,
		}
		err := f.Write(d)
		require.NoError(t, err)
		require.Equal(t, e, a.Bytes())
		m.AssertExpectations(t)
	})

	t.Run("many bytes", func(t *testing.T) {
		// 3 - full frame count
		// 512 - data fail for last frame
		// 10*1024 - max frame size
		d := make([]byte, 512+3*10*1024)
		copy(d[3*10*1024:], []byte{0x1, 0x2, 0x3, 0x4})
		f, m := prepareFramingLayer()

		a := bytes.Buffer{}
		m.On("Write", mock.Anything).Return(0, nil).Times(10).Run(func(args mock.Arguments) {
			_, err := a.Write(args[0].([]byte))
			require.NoError(t, err)
		})

		err := f.Write(d)
		require.NoError(t, err)
		// Serial
		require.Equal(t, []byte{0x0, 0x0, 0x0, 0x1}, a.Bytes()[0:4])
		// Frame count
		require.Equal(t, []byte{0x0, 0x0, 0x0, 0x4}, a.Bytes()[4:8])
		// Frame length
		require.Equal(t, []byte{0x0, 0x0, 0x28, 0x0}, a.Bytes()[8:12])
		require.Equal(t, []byte{0x0, 0x0, 0x28, 0x0}, a.Bytes()[10252:10256])
		require.Equal(t, []byte{0x0, 0x0, 0x28, 0x0}, a.Bytes()[20496:20500])
		require.Equal(t, []byte{0x0, 0x0, 0x2, 0x0}, a.Bytes()[30740:30744])
		// Last frame content (only four bytes)
		require.Equal(t, []byte{0x1, 0x2, 0x3, 0x4}, a.Bytes()[30744:30748])
		m.AssertExpectations(t)
	})

	t.Run("frame serial", func(t *testing.T) {
		d := []byte{0x1, 0x2, 0x3, 0x4}
		f, m := prepareFramingLayer()

		a := bytes.Buffer{}
		m.On("Write", mock.Anything).Return(0, nil).Times(12).Run(func(args mock.Arguments) {
			_, err := a.Write(args[0].([]byte))
			require.NoError(t, err)
		})

		err := f.Write(d)
		require.NoError(t, err)
		err = f.Write(d)
		require.NoError(t, err)
		err = f.Write(d)
		require.NoError(t, err)
		// Lats request serial
		require.Equal(t, []byte{0x0, 0x0, 0x0, 0x3}, a.Bytes()[32:36])
		m.AssertExpectations(t)
	})

	t.Run("transport error", func(t *testing.T) {
		d := []byte{0x1, 0x2, 0x3, 0x4}
		f, m := prepareFramingLayer()

		m.On("Write", mock.Anything).Return(0, fmt.Errorf("test error")).Once()

		err := f.Write(d)
		require.EqualError(t, err, "test error")
		m.AssertExpectations(t)
	})
}

func TestFramingLayer_Close(t *testing.T) {
	t.Run("succeed", func(t *testing.T) {
		f, m := prepareFramingLayer()

		m.On("Close").Return(nil).Once()

		err := f.Close()
		require.NoError(t, err)
		m.AssertExpectations(t)
	})

	t.Run("failed", func(t *testing.T) {
		f, m := prepareFramingLayer()

		m.On("Close").Return(fmt.Errorf("test error")).Once()

		err := f.Close()
		require.EqualError(t, err, "test error")
		m.AssertExpectations(t)
	})
}

func TestFramingLayer_SetDeadline(t *testing.T) {
	d := time.Now()
	f, m := prepareFramingLayer()

	m.On("SetDeadline", d).Return(nil).Once()

	err := f.SetDeadline(d)
	require.NoError(t, err)
	m.AssertExpectations(t)
}
