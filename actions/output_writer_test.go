package actions_test

import (
	"context"
	"io"

	"github.com/stretchr/testify/mock"
)

type MockOutputWriter struct {
	mock.Mock
}

func (m *MockOutputWriter) OutputWrite(ctx context.Context, name string, reader io.Reader) error {
	args := m.Mock.Called(ctx, name, reader)
	return args.Error(0)
}
