package avroipc

import (
	"errors"
	"testing"

	"github.com/myzhan/avroipc/mocks"
	"github.com/stretchr/testify/require"
)

func prepare() (*client, *mocks.MockFramingLayer, *mocks.MockCallProtocol, *mocks.MockHandshakeProtocol) {
	f := &mocks.MockFramingLayer{}
	p := &mocks.MockCallProtocol{}
	h := &mocks.MockHandshakeProtocol{}

	c := &client{
		framingLayer:      f,
		callProtocol:      p,
		handshakeProtocol: h,
	}

	return c, f, p, h
}

func TestClient_handshake(t *testing.T) {
	testErr := errors.New("test error")

	t.Run("succeed", func(t *testing.T) {
		c, f, _, h := prepare()

		request1 := []byte{0x0A, 0x0B}
		request2 := []byte{0x1A, 0x1B}
		response1 := []byte{0x2A, 0x2B}
		response2 := []byte{0x3A, 0x3B}

		// The first handshake request: emulate an unknown client protocol
		h.On("PrepareRequest").Return(request1, nil).Once()
		f.On("Write", request1).Return(nil).Once()
		f.On("Read").Return(response1, nil).Once()
		h.On("ProcessResponse", response1).Return(true, nil).Once()

		// The second handshake request: the server already knows the client protocol
		h.On("PrepareRequest").Return(request2, nil).Once()
		f.On("Write", request2).Return(nil).Once()
		f.On("Read").Return(response2, nil).Once()
		h.On("ProcessResponse", response2).Return(false, nil).Once()

		err := c.handshake()
		require.NoError(t, err)
		h.AssertExpectations(t)
		f.AssertExpectations(t)
	})

	t.Run("preparing request failed", func(t *testing.T) {
		c, f, _, h := prepare()

		request := []byte{}

		// The first handshake request: emulate an unknown client protocol
		h.On("PrepareRequest").Return(request, testErr).Once()

		err := c.handshake()
		require.EqualError(t, err, "test error")
		h.AssertExpectations(t)
		f.AssertExpectations(t)
	})
}

func TestClient_Append(t *testing.T) {
	method := "append"

	request := []byte{0x0A, 0x0B}
	response := []byte{0x1A, 0x1B}
	remaining := []byte{0x2A, 0x2B}

	origEvent := &Event{headers: map[string]string{}, body: []byte("test body")}
	prepEvent := origEvent.toMap()

	t.Run("succeed", func(t *testing.T) {
		c, f, p, _ := prepare()

		p.On("PrepareRequest", method, prepEvent).Return(request, nil)
		f.On("Write", request).Return(nil)
		f.On("Read").Return(response, nil)
		p.On("ParseResponse", method, response).Return("SOME", []byte{}, nil)

		status, err := c.Append(origEvent)
		require.NoError(t, err)
		require.Equal(t, "SOME", status)
		p.AssertExpectations(t)
		f.AssertExpectations(t)
	})

	t.Run("incorrect status type", func(t *testing.T) {
		c, f, p, _ := prepare()

		p.On("PrepareRequest", method, prepEvent).Return(request, nil).Once()
		f.On("Write", request).Return(nil).Once()
		f.On("Read").Return(response, nil).Once()
		p.On("ParseResponse", method, response).Return(0, []byte{}, nil).Once()

		status, err := c.Append(origEvent)
		require.EqualError(t, err, "cannot convert status to string: 0")
		require.Equal(t, "", status)
		p.AssertExpectations(t)
		f.AssertExpectations(t)
	})

	t.Run("non-empty response buffer", func(t *testing.T) {
		c, f, p, _ := prepare()

		p.On("PrepareRequest", method, prepEvent).Return(request, nil).Once()
		f.On("Write", request).Return(nil).Once()
		f.On("Read").Return(response, nil).Once()
		p.On("ParseResponse", method, response).Return("SOME", remaining, nil).Once()

		status, err := c.Append(origEvent)
		require.EqualError(t, err, "response buffer is not empty: len=2, rest=0x2A2B")
		require.Equal(t, "", status)
		p.AssertExpectations(t)
		f.AssertExpectations(t)
	})
}

func TestClient_AppendBatch(t *testing.T) {
	method := "appendBatch"

	request := []byte{0x0A, 0x0B}
	response := []byte{0x1A, 0x1B}
	remaining := []byte{0x2A, 0x2B}

	origEvents := []*Event{
		{headers: map[string]string{}, body: []byte("test body 1")},
		{headers: map[string]string{}, body: []byte("test body 2")},
	}
	prepEvents := []map[string]interface{}{
		origEvents[0].toMap(),
		origEvents[1].toMap(),
	}

	t.Run("succeed", func(t *testing.T) {
		c, f, p, _ := prepare()

		p.On("PrepareRequest", method, prepEvents).Return(request, nil)
		f.On("Write", request).Return(nil)
		f.On("Read").Return(response, nil)
		p.On("ParseResponse", method, response).Return("SOME", []byte{}, nil)

		status, err := c.AppendBatch(origEvents)
		require.NoError(t, err)
		require.Equal(t, "SOME", status)
		p.AssertExpectations(t)
		f.AssertExpectations(t)
	})

	t.Run("incorrect status type", func(t *testing.T) {
		c, f, p, _ := prepare()

		p.On("PrepareRequest", method, prepEvents).Return(request, nil).Once()
		f.On("Write", request).Return(nil).Once()
		f.On("Read").Return(response, nil).Once()
		p.On("ParseResponse", method, response).Return(0, []byte{}, nil).Once()

		status, err := c.AppendBatch(origEvents)
		require.EqualError(t, err, "cannot convert status to string: 0")
		require.Equal(t, "", status)
		p.AssertExpectations(t)
		f.AssertExpectations(t)
	})

	t.Run("non-empty response buffer", func(t *testing.T) {
		c, f, p, _ := prepare()

		p.On("PrepareRequest", method, prepEvents).Return(request, nil).Once()
		f.On("Write", request).Return(nil).Once()
		f.On("Read").Return(response, nil).Once()
		p.On("ParseResponse", method, response).Return("SOME", remaining, nil).Once()

		status, err := c.AppendBatch(origEvents)
		require.EqualError(t, err, "response buffer is not empty: len=2, rest=0x2A2B")
		require.Equal(t, "", status)
		p.AssertExpectations(t)
		f.AssertExpectations(t)
	})
}

func TestClient_Close(t *testing.T) {
	testErr := errors.New("test error")

	t.Run("succeed", func(t *testing.T) {
		c, f, _, _ := prepare()

		f.On("Close").Return(nil)

		err := c.Close()
		require.NoError(t, err)
		f.AssertExpectations(t)
	})

	t.Run("framing layer error", func(t *testing.T) {
		c, f, _, _ := prepare()

		f.On("Close").Return(testErr)

		err := c.Close()
		require.EqualError(t, err, "test error")
		f.AssertExpectations(t)
	})
}
