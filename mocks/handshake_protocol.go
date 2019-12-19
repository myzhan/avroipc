package mocks

import (
	"github.com/stretchr/testify/mock"
)

type MockHandshakeProtocol struct {
	mock.Mock
}

func (p *MockHandshakeProtocol) PrepareRequest() ([]byte, error) {
	args := p.Called()
	return args.Get(0).([]byte), args.Error(1)
}

func (p *MockHandshakeProtocol) ProcessResponse(responseBytes []byte) (bool, error) {
	args := p.Called(responseBytes)
	return args.Bool(0), args.Error(1)
}
