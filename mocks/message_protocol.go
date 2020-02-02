package mocks

import "github.com/stretchr/testify/mock"

type MockProtocol struct {
	mock.Mock
}

func (p *MockProtocol) PrepareMessage(method string, datum interface{}) ([]byte, error) {
	args := p.Called(method, datum)
	return args.Get(0).([]byte), args.Error(1)
}

func (p *MockProtocol) ParseMessage(method string, responseBytes []byte) (interface{}, []byte, error) {
	args := p.Called(method, responseBytes)
	return args.Get(0), args.Get(1).([]byte), args.Error(2)
}

func (p *MockProtocol) ParseError(method string, responseBytes []byte) ([]byte, error) {
	args := p.Called(method, responseBytes)
	return args.Get(0).([]byte), args.Error(1)
}

func (p *MockProtocol) GetSchema() string {
	args := p.Called()
	return args.String(0)
}
