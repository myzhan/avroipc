package avroipc

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

func (e *Event) toMap() map[string]interface{} {
	m := make(map[string]interface{})
	m["headers"] = e.headers
	m["body"] = e.body

	return m
}
