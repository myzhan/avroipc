package mocks

import (
	"github.com/stretchr/testify/mock"
	"time"
)

type MockTransport struct {
	mock.Mock
}

func (t *MockTransport) Open() error {
	args := t.Called()
	return args.Error(0)
}

func (t *MockTransport) Close() error {
	args := t.Called()
	return args.Error(0)
}

func (t *MockTransport) Read(p []byte) (n int, err error) {
	args := t.Called(p)
	return args.Int(0), args.Error(1)
}

func (t *MockTransport) Write(p []byte) (n int, err error) {
	args := t.Called(p)
	return args.Int(0), args.Error(1)
}

func (t *MockTransport) Flush() error {
	args := t.Called()
	return args.Error(0)
}

func (t *MockTransport) SetDeadline(d time.Time) error {
	args := t.Called(d)
	return args.Error(0)
}
