package mocks

import "github.com/stretchr/testify/mock"

type MockCallProtocol struct {
	mock.Mock
}

func (p *MockCallProtocol) PrepareRequest(method string, datum interface{}) ([]byte, error) {
	args := p.Called(method, datum)
	return args.Get(0).([]byte), args.Error(1)
}

func (p *MockCallProtocol) ParseResponse(method string, responseBytes []byte) (interface{}, error) {
	args := p.Called(method, responseBytes)
	return args.Get(0), args.Error(1)
}
