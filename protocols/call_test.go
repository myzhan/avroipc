package protocols_test

import (
	"errors"
	"testing"

	"github.com/myzhan/avroipc/mocks"
	"github.com/stretchr/testify/require"

	"github.com/myzhan/avroipc/protocols"
)

func prepareCallProtocol(t *testing.T) (protocols.CallProtocol, *mocks.MockProtocol) {
	m := &mocks.MockProtocol{}

	p, err := protocols.NewCall(m)
	require.NoError(t, err)

	return p, m
}

// Test successful schema compilation
func TestNewCall(t *testing.T) {
	_, err := protocols.NewCall(nil)
	require.NoError(t, err)
}

func TestCallProtocol_PrepareRequest(t *testing.T) {
	datum := []byte{0xA, 0xB, 0xC}
	message := []byte{0xD, 0xE, 0xF}
	emptyMethod := ""
	appendMethod := "append"

	nilError := error(nil)
	testError := errors.New("test error")

	t.Run("empty method", func(t *testing.T) {
		p, m := prepareCallProtocol(t)
		m.On("PrepareMessage", emptyMethod, datum).Return(message, nilError).Once()

		actual, err := p.PrepareRequest(emptyMethod, datum)
		require.NoError(t, err)
		require.Equal(t, []byte{0x0, 0x0, 0xD, 0xE, 0xF}, actual)

		m.AssertExpectations(t)
	})
	t.Run("append method", func(t *testing.T) {
		p, m := prepareCallProtocol(t)
		m.On("PrepareMessage", appendMethod, datum).Return(message, nilError).Once()

		actual, err := p.PrepareRequest(appendMethod, datum)
		require.NoError(t, err)
		require.Equal(t, []byte{0x0, 0xc, 0x61, 0x70, 0x70, 0x65, 0x6e, 0x64, 0xD, 0xE, 0xF}, actual)

		m.AssertExpectations(t)
	})
	t.Run("protocol error", func(t *testing.T) {
		p, m := prepareCallProtocol(t)
		m.On("PrepareMessage", "append", datum).Return(message, testError).Once()

		_, err := p.PrepareRequest("append", datum)
		require.EqualError(t, err, "test error")

		m.AssertExpectations(t)
	})
}

func TestCallProtocol_ParseResponse(t *testing.T) {
	status := "test status"
	method := "test method"

	rest := []byte{}
	data := append([]byte{0xA, 0xB, 0xC}, rest...)

	longRest := []byte{0xD, 0xE, 0xF}
	longData := append([]byte{}, longRest...)

	okResponse := append([]byte{0x0, 0x0}, data...)
	badResponse := append([]byte{0x0, 0x2}, data...)
	longResponse := append([]byte{0x0, 0x0}, longData...)
	shortResponse := []byte{0x0}
	errorResponse := append([]byte{0x0, 0x1}, data...)
	errorLongResponse := append([]byte{0x0, 0x1}, longData...)

	nilError := error(nil)
	testError := errors.New("test error")

	t.Run("success", func(t *testing.T) {
		p, m := prepareCallProtocol(t)
		m.On("ParseMessage", method, data).Return(status, rest, nilError).Once()

		actual, err := p.ParseResponse(method, okResponse)
		require.NoError(t, err)
		require.Equal(t, status, actual)

		m.AssertExpectations(t)
	})

	t.Run("bad flag", func(t *testing.T) {
		p, m := prepareCallProtocol(t)

		_, err := p.ParseResponse(method, badResponse)
		require.EqualError(t, err, "cannot decode binary boolean: expected: Go byte(0) or byte(1); received: byte(2)")

		m.AssertExpectations(t)
	})

	t.Run("short buffer", func(t *testing.T) {
		p, m := prepareCallProtocol(t)

		_, err := p.ParseResponse(method, shortResponse)
		require.EqualError(t, err, "short buffer")

		m.AssertExpectations(t)
	})

	t.Run("process error", func(t *testing.T) {
		p, m := prepareCallProtocol(t)
		m.On("ParseError", method, data).Return(rest, testError).Once()

		_, err := p.ParseResponse(method, errorResponse)
		require.EqualError(t, err, "test error")

		m.AssertExpectations(t)
	})

	t.Run("buffer not empty", func(t *testing.T) {
		p, m := prepareCallProtocol(t)
		m.On("ParseMessage", method, longData).Return(status, longRest, nilError).Once()

		_, err := p.ParseResponse(method, longResponse)
		require.EqualError(t, err, "response buffer is not empty: len=3, rest=0x0D0E0F")

		m.AssertExpectations(t)
	})

	t.Run("process error with non-empty buffer", func(t *testing.T) {
		p, m := prepareCallProtocol(t)
		m.On("ParseError", method, longData).Return(longRest, nilError).Once()

		_, err := p.ParseResponse(method, errorLongResponse)
		require.EqualError(t, err, "response buffer is not empty: len=3, rest=0x0D0E0F")

		m.AssertExpectations(t)
	})
}
