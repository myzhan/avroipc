package avroipc

import (
	"errors"
	"testing"

	"github.com/myzhan/avroipc/mocks"

	"github.com/stretchr/testify/require"
)

func prepare() (*client, *mocks.MockTransport, *mocks.MockFramingLayer, *mocks.MockCallProtocol, *mocks.MockHandshakeProtocol) {
	t := &mocks.MockTransport{}
	f := &mocks.MockFramingLayer{}
	p := &mocks.MockCallProtocol{}
	h := &mocks.MockHandshakeProtocol{}

	c := &client{
		transport:         t,
		framingLayer:      f,
		callProtocol:      p,
		handshakeProtocol: h,
	}

	return c, t, f, p, h
}

func TestClient_handshake(t *testing.T) {
	testErr := errors.New("test error")

	t.Run("succeed", func(t *testing.T) {
		c, x, f, _, h := prepare()

		request1 := []byte{0x0A, 0x0B}
		request2 := []byte{0x1A, 0x1B}
		response1 := []byte{0x2A, 0x2B}
		response2 := []byte{0x3A, 0x3B}

		// The first handshake request: emulate an unknown client protocol
		h.On("PrepareRequest").Return(request1, nil).Once()
		f.On("Write", request1).Return(nil).Once()
		x.On("Flush").Return(nil).Once()
		f.On("Read").Return(response1, nil).Once()
		h.On("ProcessResponse", response1).Return(true, nil).Once()

		// The second handshake request: the server already knows the client protocol
		h.On("PrepareRequest").Return(request2, nil).Once()
		f.On("Write", request2).Return(nil).Once()
		x.On("Flush").Return(nil).Once()
		f.On("Read").Return(response2, nil).Once()
		h.On("ProcessResponse", response2).Return(false, nil).Once()

		err := c.handshake()
		require.NoError(t, err)
		h.AssertExpectations(t)
		f.AssertExpectations(t)
		x.AssertExpectations(t)
	})

	t.Run("preparing request failed", func(t *testing.T) {
		c, _, f, _, h := prepare()

		request := []byte{}

		// The first handshake request: emulate an unknown client protocol
		h.On("PrepareRequest").Return(request, testErr).Once()

		err := c.handshake()
		require.EqualError(t, err, "test error")
		h.AssertExpectations(t)
		f.AssertExpectations(t)
	})
}

func TestClient_Close(t *testing.T) {
	testErr := errors.New("test error")

	t.Run("succeed", func(t *testing.T) {
		c, x, _, _, _ := prepare()

		x.On("Close").Return(nil).Once()

		err := c.Close()
		require.NoError(t, err)
		x.AssertExpectations(t)
	})

	t.Run("framing layer error", func(t *testing.T) {
		c, x, _, _, _ := prepare()

		x.On("Close").Return(testErr).Once()

		err := c.Close()
		require.EqualError(t, err, "test error")
		x.AssertExpectations(t)
	})
}

func TestClient_SendMessage(t *testing.T) {
	datum := "test data"
	method := "append"

	request := []byte{0x0A, 0x0B}
	response := []byte{0x1A, 0x1B}

	t.Run("succeed", func(t *testing.T) {
		c, x, f, p, _ := prepare()

		p.On("PrepareRequest", method, datum).Return(request, nil).Once()
		f.On("Write", request).Return(nil).Once()
		x.On("Flush").Return(nil).Once()
		f.On("Read").Return(response, nil).Once()
		p.On("ParseResponse", method, response).Return("SOME", nil).Once()

		status, err := c.SendMessage(method, datum)
		require.NoError(t, err)
		require.Equal(t, "SOME", status)
		p.AssertExpectations(t)
		f.AssertExpectations(t)
		x.AssertExpectations(t)
	})

	t.Run("incorrect status type", func(t *testing.T) {
		c, x, f, p, _ := prepare()

		p.On("PrepareRequest", method, datum).Return(request, nil).Once()
		f.On("Write", request).Return(nil).Once()
		x.On("Flush").Return(nil).Once()
		f.On("Read").Return(response, nil).Once()
		p.On("ParseResponse", method, response).Return(0, nil).Once()

		status, err := c.SendMessage(method, datum)
		require.EqualError(t, err, "cannot convert status to string: 0")
		require.Equal(t, "", status)
		p.AssertExpectations(t)
		f.AssertExpectations(t)
		x.AssertExpectations(t)
	})
}
