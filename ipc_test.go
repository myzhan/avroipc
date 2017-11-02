package avroipc

import (
	"log"
	"testing"
)

func TestSend(t *testing.T) {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	// flume avro instance address
	client := NewClient("localhost:20200")

	headersMap := make(map[string]string)
	headersMap["topic"] = "myzhan"
	headersMap["timestamp"] = "1508740315478"
	body := []byte("hello from go")

	event := NewEvent(headersMap, body)

	client.Append(event)
}
