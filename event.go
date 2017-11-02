package avroipc

import "log"


// Event acts as an avro event
type Event struct {
	headers map[string]string
	body    []byte
}

// NewEvent creates an Event, which can be sent to flume
func NewEvent(headers map[string]string, body []byte) *Event {
	return &Event{
		headers: headers,
		body:    body,
	}
}

// Bytes converts event to byte array
func (event *Event) Bytes() []byte {
	avroFlumeEvent := make(map[string]interface{})
	avroFlumeEvent["headers"] = event.headers
	avroFlumeEvent["body"] = event.body
	bin, err := eventCodec.BinaryFromNative(nil, avroFlumeEvent)
	if err != nil {
		log.Fatalf("%v", err)
	}
	return bin
}
