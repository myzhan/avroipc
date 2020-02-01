package mocks

import (
	"github.com/stretchr/testify/mock"
)

type MockClient struct {
	mock.Mock
}

func (c *MockClient) Close() error {
	args := c.Called()
	return args.Error(0)
}

func (c *MockClient) SendMessage(method string, datum interface{}) (string, error) {
	args := c.Called(method, datum)
	return args.String(0), args.Error(1)
}
