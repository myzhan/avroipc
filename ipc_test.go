package avroipc

import (
	"log"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSend(t *testing.T) {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	addr := os.Getenv("FLUME_SERVER_ADDRESS")
	if addr == "" {
		t.Skip("The FLUME_SERVER_ADDRESS environment variable is not set")
	}

	level := os.Getenv("FLUME_COMPRESSION_LEVEL")
	levelInt := 0
	if level != "" {
		var err error
		levelInt, err = strconv.Atoi(level)
		require.NoError(t, err)
	}

	bufferSize := 8
	sendTimeout := time.Duration(0)

	// flume avro instance address
	client, err := NewClient(addr, sendTimeout, bufferSize, levelInt)
	require.NoError(t, err)

	event := &Event{
		Body: []byte("hi from go"),
		Headers: map[string]string{
			"topic":     "myzhan",
			"timestamp": "1508740315478",
		},
	}
	events := []*Event{
		event,
		{
			Body: []byte("hello from go"),
			Headers: map[string]string{
				"topic":     "vykulakov",
				"timestamp": "1576795153258",
			},
		},
	}

	var status string

	t.Run("test append", func(t *testing.T) {
		// The first append call.
		status, err = client.Append(event)
		require.NoError(t, err)
		require.Equal(t, "OK", status)

		// The second append call.
		status, err = client.Append(event)
		require.NoError(t, err)
		require.Equal(t, "OK", status)
	})

	t.Run("test appendBatch", func(t *testing.T) {
		// The first append call.
		status, err = client.AppendBatch(events)
		require.NoError(t, err)
		require.Equal(t, "OK", status)

		// The second append call.
		status, err = client.AppendBatch(events)
		require.NoError(t, err)
		require.Equal(t, "OK", status)
	})

	// Close the client finally.
	require.NoError(t, client.Close())
}
