package mocks

import (
	"github.com/stretchr/testify/mock"
)

type MockFramingLayer struct {
	mock.Mock
}

func (f *MockFramingLayer) Read() ([]byte, error) {
	args := f.Called()
	return args.Get(0).([]byte), args.Error(1)
}

func (f *MockFramingLayer) Write(p []byte) error {
	args := f.Called(p)
	return args.Error(0)
}
