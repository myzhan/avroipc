package avroipc_test

import (
	"errors"
	"testing"

	"github.com/myzhan/avroipc"
	"github.com/myzhan/avroipc/mocks"
	"github.com/stretchr/testify/require"
)

func TestCallProtocol_PrepareRequest(t *testing.T) {
	m := &mocks.MockProtocol{}
	m.On("PrepareMessage").Return([]byte{}, nil)

	p, err := avroipc.NewCallProtocol(m)
	require.NoError(t, err)

	t.Run("empty method", func(t *testing.T) {
		actual, err := p.PrepareRequest("", nil)
		require.NoError(t, err)
		require.Equal(t, []byte{0x0, 0x0}, actual)
	})
	t.Run("append method", func(t *testing.T) {
		actual, err := p.PrepareRequest("append", nil)
		require.NoError(t, err)
		require.Equal(t, []byte{0x0, 0xc, 0x61, 0x70, 0x70, 0x65, 0x6e, 0x64}, actual)
	})
	t.Run("protocol error", func(t *testing.T) {
		m := &mocks.MockProtocol{}
		m.On("PrepareMessage").Return([]byte{}, errors.New("test error"))

		p, err := avroipc.NewCallProtocol(m)
		require.NoError(t, err)

		_, err = p.PrepareRequest("append", nil)
		require.EqualError(t, err, "test error")

		m.AssertExpectations(t)
	})
}

func TestCallProtocol_ParseResponse(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		m := &mocks.MockProtocol{}
		m.On("ParseMessage").Return("test", []byte{}, nil)

		p, err := avroipc.NewCallProtocol(m)
		require.NoError(t, err)

		actual, err := p.ParseResponse("", []byte{0x0, 0x0})
		require.NoError(t, err)
		require.Equal(t, "test", actual)

		m.AssertExpectations(t)
	})

	t.Run("short buffer", func(t *testing.T) {
		m := &mocks.MockProtocol{}

		p, err := avroipc.NewCallProtocol(m)
		require.NoError(t, err)

		_, err = p.ParseResponse("", []byte{0x0})
		require.EqualError(t, err, "short buffer")

		m.AssertExpectations(t)
	})

	t.Run("bad flag", func(t *testing.T) {
		m := &mocks.MockProtocol{}

		p, err := avroipc.NewCallProtocol(m)
		require.NoError(t, err)

		_, err = p.ParseResponse("", []byte{0x0, 0x2})
		require.Error(t, err)
		require.Contains(t, err.Error(), "cannot decode binary boolean")

		m.AssertExpectations(t)
	})

	t.Run("process error", func(t *testing.T) {
		m := &mocks.MockProtocol{}
		m.On("ParseError").Return([]byte{}, errors.New("test error"))

		p, err := avroipc.NewCallProtocol(m)
		require.NoError(t, err)

		_, err = p.ParseResponse("", []byte{0x0, 0x1})
		require.EqualError(t, err, "test error")

		m.AssertExpectations(t)
	})
}
