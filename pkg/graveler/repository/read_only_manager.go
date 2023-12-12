package repository

import (
	"context"
	"github.com/treeverse/lakefs/pkg/graveler"
)

type ReadOnlyRepositoriesManager struct {
}

func NewReadOnlyRepositoriesManager() *ReadOnlyRepositoriesManager {
	return &ReadOnlyRepositoriesManager{}
}

func (m *ReadOnlyRepositoriesManager) IsBlocked(ctx context.Context, repository *graveler.RepositoryRecord) bool {
	return repository.ReadOnly
}
