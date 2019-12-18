package avroipc_test

import (
	"fmt"
	"github.com/myzhan/avroipc"
	"github.com/myzhan/avroipc/mocks"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestCallProtocol_PrepareRequest(t *testing.T) {
	p, err := avroipc.NewCallProtocol(&mocks.MockProtocol{})
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
		p, err := avroipc.NewCallProtocol(&mocks.MockProtocol{
			Err: fmt.Errorf("test error"),
		})
		require.NoError(t, err)

		_, err = p.PrepareRequest("append", nil)
		require.EqualError(t, err, "test error")
	})
}

func TestCallProtocol_ParseResponse(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		expected := "test"
		p, err := avroipc.NewCallProtocol(&mocks.MockProtocol{
			Response:           expected,
			ParseResponseBytes: []byte{0x77, 0x78},
		})
		require.NoError(t, err)

		actual, bytes, err := p.ParseResponse("", []byte{0x0, 0x0})
		require.NoError(t, err)
		require.Equal(t, []byte{0x77, 0x78}, bytes)
		require.Equal(t, expected, actual)
	})

	t.Run("short buffer", func(t *testing.T) {
		expected := "test"
		p, err := avroipc.NewCallProtocol(&mocks.MockProtocol{
			Response:           expected,
			ParseResponseBytes: []byte{0x77, 0x78},
		})
		require.NoError(t, err)

		_, bytes, err := p.ParseResponse("", []byte{0x0})
		require.EqualError(t, err, "short buffer")
		require.Equal(t, []byte{}, bytes)
	})

	t.Run("bad flag", func(t *testing.T) {
		expected := "test"
		p, err := avroipc.NewCallProtocol(&mocks.MockProtocol{
			Response:           expected,
			ParseResponseBytes: []byte{0x77, 0x78},
		})
		require.NoError(t, err)

		_, bytes, err := p.ParseResponse("", []byte{0x0, 0x2})
		require.Error(t, err)
		require.Contains(t, err.Error(), "cannot decode binary boolean")
		require.Equal(t, []byte{0x2}, bytes)
	})

	t.Run("process error", func(t *testing.T) {
		expected := fmt.Errorf("test error")
		p, err := avroipc.NewCallProtocol(&mocks.MockProtocol{
			Err:                expected,
			ErrorResponseBytes: []byte{0x88, 0x89},
		})
		require.NoError(t, err)

		_, bytes, err := p.ParseResponse("", []byte{0x0, 0x1})
		require.EqualError(t, err, "test error")
		require.Equal(t, []byte{0x88, 0x89}, bytes)
	})
}
