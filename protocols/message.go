package protocols

// The interface for all Avro RPC protocol implementations.
type MessageProtocol interface {
	PrepareMessage(method string, datum interface{}) ([]byte, error)
	ParseMessage(method string, responseBytes []byte) (interface{}, []byte, error)
	ParseError(method string, responseBytes []byte) ([]byte, error)
	GetSchema() string
}
