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

	headersMap := make(map[string]string)
	headersMap["topic"] = "myzhan"
	headersMap["timestamp"] = "1508740315478"
	body := []byte("hello from go")

	event := NewEvent(headersMap, body)

	status, err := client.Append(event)
	require.NoError(t, err)
	require.Equal(t, "OK", status)
}
