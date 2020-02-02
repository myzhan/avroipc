package protocols

import (
	"testing"

	"github.com/myzhan/avroipc/mocks"

	"github.com/stretchr/testify/require"
)

func prepareHandshakeProtocol(t *testing.T) (HandshakeProtocol, *mocks.MockProtocol) {
	m := &mocks.MockProtocol{}
	m.On("GetSchema").Return("test schema").Once()

	h, err := NewHandshake(m)
	require.NoError(t, err)

	return h, m
}

func Test_getMD5(t *testing.T) {
	t.Run("empty value", func(t *testing.T) {
		actual := getMD5("")
		expected := []byte{0xd4, 0x1d, 0x8c, 0xd9, 0x8f, 0x0, 0xb2, 0x4, 0xe9, 0x80, 0x9, 0x98, 0xec, 0xf8, 0x42, 0x7e}

		require.Equal(t, expected, actual)
	})

	t.Run("normal value", func(t *testing.T) {
		actual := getMD5("test string")
		expected := []byte{0x6f, 0x8d, 0xb5, 0x99, 0xde, 0x98, 0x6f, 0xab, 0x7a, 0x21, 0x62, 0x5b, 0x79, 0x16, 0x58, 0x9c}

		require.Equal(t, expected, actual)
	})
}

// Test successful schema compilation
func TestNewHandshake(t *testing.T) {
	_, m := prepareHandshakeProtocol(t)
	m.AssertExpectations(t)
}

func TestHandshakeProtocol_PrepareRequest(t *testing.T) {
	t.Run("without client protocol", func(t *testing.T) {
		expected := []byte{
			// Client hash.
			0xc2, 0x20, 0xbe, 0x3a, 0x18, 0x60, 0xad, 0xac, 0xc6, 0x49, 0xc2, 0x5e, 0xba, 0x89, 0x97, 0x59,
			// Client protocol.
			0x0,
			// Server hash.
			0xc2, 0x20, 0xbe, 0x3a, 0x18, 0x60, 0xad, 0xac, 0xc6, 0x49, 0xc2, 0x5e, 0xba, 0x89, 0x97, 0x59,
			// Metadata
			0x0,
			// Empty message.
			0x0, 0x0,
		}

		p, m := prepareHandshakeProtocol(t)

		actual, err := p.PrepareRequest()
		require.NoError(t, err)
		require.Equal(t, expected, actual)
		m.AssertExpectations(t)
	})

	t.Run("with client protocol", func(t *testing.T) {
		expected := []byte{
			// Client hash.
			0xc2, 0x20, 0xbe, 0x3a, 0x18, 0x60, 0xad, 0xac, 0xc6, 0x49, 0xc2, 0x5e, 0xba, 0x89, 0x97, 0x59,
			// Client protocol.
			0x02, 0x16, 0x74, 0x65, 0x73, 0x74, 0x20, 0x73, 0x63, 0x68, 0x65, 0x6d, 0x61,
			// Server hash.
			0xc2, 0x20, 0xbe, 0x3a, 0x18, 0x60, 0xad, 0xac, 0xc6, 0x49, 0xc2, 0x5e, 0xba, 0x89, 0x97, 0x59,
			// Metadata
			0x0,
			// Empty message.
			0x0, 0x0,
		}

		p, m := prepareHandshakeProtocol(t)

		h := p.(*handshakeProtocol)
		h.needClientProtocol = true

		actual, err := p.PrepareRequest()
		require.NoError(t, err)
		require.Equal(t, expected, actual)
		m.AssertExpectations(t)
	})
}

func TestHandshakeProtocol_ProcessResponse(t *testing.T) {
	t.Run("bad match", func(t *testing.T) {
		response := []byte{
			// Match.
			0x7,
		}

		p, m := prepareHandshakeProtocol(t)

		needResend, err := p.ProcessResponse(response)
		require.Error(t, err)
		require.Contains(t, err.Error(), "cannot decode binary enum")
		require.False(t, needResend)
		m.AssertExpectations(t)
	})

	t.Run("short buffer", func(t *testing.T) {
		response := []byte{
			// Match.
			0x0,
		}

		p, m := prepareHandshakeProtocol(t)

		needResend, err := p.ProcessResponse(response)
		require.Error(t, err)
		require.Contains(t, err.Error(), "short buffer")
		require.False(t, needResend)
		m.AssertExpectations(t)
	})

	t.Run("both match", func(t *testing.T) {
		response := []byte{
			// Match.
			0x0,
			// Server protocol.
			0x0,
			// Server hash.
			0x0,
			// Metadata
			0x0,
		}

		p, m := prepareHandshakeProtocol(t)

		needResend, err := p.ProcessResponse(response)
		require.NoError(t, err)
		require.False(t, needResend)
		m.AssertExpectations(t)
	})

	t.Run("none match", func(t *testing.T) {
		response := []byte{
			// Match.
			0x4,
			// Server protocol.
			0x0,
			// Server hash.
			0x2, 0x49, 0x87, 0x43, 0x7b, 0xf5, 0x9, 0xdf, 0xde, 0x62, 0x36, 0x72, 0x99, 0xef, 0x40, 0xc8, 0x2f,
			// Metadata
			0x0,
		}

		p, m := prepareHandshakeProtocol(t)

		needResend, err := p.ProcessResponse(response)
		require.NoError(t, err)
		require.True(t, needResend)
		m.AssertExpectations(t)
	})

	t.Run("repeated none match", func(t *testing.T) {
		response := []byte{
			// Match.
			0x4,
			// Server protocol.
			0x0,
			// Server hash.
			0x2, 0x49, 0x87, 0x43, 0x7b, 0xf5, 0x9, 0xdf, 0xde, 0x62, 0x36, 0x72, 0x99, 0xef, 0x40, 0xc8, 0x2f,
			// Metadata
			0x0,
		}

		p, m := prepareHandshakeProtocol(t)

		h := p.(*handshakeProtocol)
		h.needClientProtocol = true

		needResend, err := p.ProcessResponse(response)
		require.Error(t, err)
		require.Contains(t, err.Error(), "unknown client's protocol")
		require.False(t, needResend)
		m.AssertExpectations(t)
	})

	t.Run("client match", func(t *testing.T) {
		response := []byte{
			// Match.
			0x2,
			// Server protocol.
			0x2, 0x8, 0x74, 0x65, 0x73, 0x74,
			// Server hash.
			0x2, 0x49, 0x87, 0x43, 0x7b, 0xf5, 0x9, 0xdf, 0xde, 0x62, 0x36, 0x72, 0x99, 0xef, 0x40, 0xc8, 0x2f,
			// Metadata
			0x0,
		}

		p, m := prepareHandshakeProtocol(t)

		needResend, err := p.ProcessResponse(response)
		require.NoError(t, err)
		require.False(t, needResend)
		m.AssertExpectations(t)
	})

	t.Run("bad client match order", func(t *testing.T) {
		response := []byte{
			// Match.
			0x2,
			// Server protocol.
			0x2, 0x8, 0x74, 0x65, 0x73, 0x74,
			// Server hash.
			0x2, 0x49, 0x87, 0x43, 0x7b, 0xf5, 0x9, 0xdf, 0xde, 0x62, 0x36, 0x72, 0x99, 0xef, 0x40, 0xc8, 0x2f,
			// Metadata
			0x0,
		}

		p, m := prepareHandshakeProtocol(t)

		h := p.(*handshakeProtocol)
		h.needClientProtocol = true

		needResend, err := p.ProcessResponse(response)
		require.Error(t, err)
		require.Contains(t, err.Error(), "unknown client's protocol")
		require.False(t, needResend)
		m.AssertExpectations(t)
	})
}
