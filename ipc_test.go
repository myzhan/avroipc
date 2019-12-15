package avroipc

import (
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
	client := NewClient(addr)

	headersMap := make(map[string]string)
	headersMap["topic"] = "myzhan"
	headersMap["timestamp"] = "1508740315478"
	body := []byte("hello from go")

	event := NewEvent(headersMap, body)

	client.Append(event)
}
