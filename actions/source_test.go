package actions_test

import "github.com/stretchr/testify/mock"

type MockSource struct {
	mock.Mock
}

func (m *MockSource) List() ([]string, error) {
	args := m.Called()
	return args.Get(0).([]string), args.Error(1)
}

func (m *MockSource) Load(name string) ([]byte, error) {
	args := m.Called(name)
	return args.Get(0).([]byte), args.Error(1)
}
