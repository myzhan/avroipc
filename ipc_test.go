package avroipc

import (
	"github.com/stretchr/testify/require"
	"log"
	"os"
	"testing"
)

func TestSend(t *testing.T) {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	addr := os.Getenv("FLUME_SERVER_ADDRESS")
	if addr == "" {
		t.Skip("The FLUME_SERVER_ADDRESS environment variable is not set")
	}

	// flume avro instance address
	client, err := NewClient(addr)
	require.NoError(t, err)

	event := &Event{
		body: []byte("hello from go"),
		headers: map[string]string{
			"topic":     "myzhan",
			"timestamp": "1508740315478",
		},
	}

	var status string

	// The first append call.
	status, err = client.Append(event)
	require.NoError(t, err)
	require.Equal(t, "OK", status)

	// The second append call.
	status, err = client.Append(event)
	require.NoError(t, err)
	require.Equal(t, "OK", status)

	// Close the client finally.
	require.NoError(t, client.Close())
}
