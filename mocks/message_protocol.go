package mocks

type MockProtocol struct {
	Err                error
	Message            []byte
	Response           interface{}
	ParseResponseBytes []byte
	ErrorResponseBytes []byte
}

func (p *MockProtocol) PrepareMessage(method string, datum interface{}) ([]byte, error) {
	return p.Message, p.Err
}

func (p *MockProtocol) ParseMessage(method string, responseBytes []byte) (interface{}, []byte, error) {
	return p.Response, p.ParseResponseBytes, p.Err
}

func (p *MockProtocol) ParseError(method string, responseBytes []byte) ([]byte, error) {
	return p.ErrorResponseBytes, p.Err
}
