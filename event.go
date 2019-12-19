package avroipc

// Event acts as an avro event
type Event struct {
	headers map[string]string
	body    []byte
}

func (e *Event) toMap() map[string]interface{} {
	m := make(map[string]interface{})
	m["headers"] = e.headers
	m["body"] = e.body

	return m
}
