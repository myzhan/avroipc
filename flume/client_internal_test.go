package flume

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/myzhan/avroipc/flume/mocks"
)

func prepare() (*client, *mocks.MockClient) {
	x := &mocks.MockClient{}

	c := &client{
		client: x,
	}

	return c, x
}

func TestClient_Close(t *testing.T) {
	testErr := errors.New("test error")

	t.Run("succeed", func(t *testing.T) {
		c, x := prepare()

		x.On("Close").Return(nil).Once()

		err := c.Close()
		require.NoError(t, err)
		x.AssertExpectations(t)
	})

	t.Run("client error", func(t *testing.T) {
		c, x := prepare()

		x.On("Close").Return(testErr).Once()

		err := c.Close()
		require.EqualError(t, err, "test error")
		x.AssertExpectations(t)
	})
}

func TestClient_Append(t *testing.T) {
	method := "append"

	origEvent := &Event{Headers: map[string]string{}, Body: []byte("test body")}
	prepEvent := origEvent.toMap()

	t.Run("succeed", func(t *testing.T) {
		c, x := prepare()

		x.On("SendMessage", method, prepEvent).Return("SOME", nil).Once()

		status, err := c.Append(origEvent)
		require.NoError(t, err)
		require.Equal(t, "SOME", status)
		x.AssertExpectations(t)
	})
}

func TestClient_AppendBatch(t *testing.T) {
	method := "appendBatch"

	origEvents := []*Event{
		{Headers: map[string]string{}, Body: []byte("test body 1")},
		{Headers: map[string]string{}, Body: []byte("test body 2")},
	}
	prepEvents := []map[string]interface{}{
		origEvents[0].toMap(),
		origEvents[1].toMap(),
	}

	t.Run("succeed", func(t *testing.T) {
		c, x := prepare()

		x.On("SendMessage", method, prepEvents).Return("SOME", nil).Once()

		status, err := c.AppendBatch(origEvents)
		require.NoError(t, err)
		require.Equal(t, "SOME", status)
		x.AssertExpectations(t)
	})
}
