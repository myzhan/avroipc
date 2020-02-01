package flume

// Event acts as an avro event
type Event struct {
	Headers map[string]string
	Body    []byte
}

func (e *Event) toMap() map[string]interface{} {
	m := make(map[string]interface{})
	m["headers"] = e.Headers
	m["body"] = e.Body

	return m
}
