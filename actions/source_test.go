package actions_test

import (
	"context"

	"github.com/stretchr/testify/mock"
	"github.com/treeverse/lakefs/actions"
)

type MockSource struct {
	mock.Mock
}

func (m *MockSource) List(ctx context.Context) ([]actions.FileRef, error) {
	args := m.Called(ctx)
	return args.Get(0).([]actions.FileRef), args.Error(1)
}

func (m *MockSource) Load(ctx context.Context, name actions.FileRef) ([]byte, error) {
	args := m.Called(ctx, name)
	return args.Get(0).([]byte), args.Error(1)
}
