package repository

import (
	"github.com/treeverse/lakefs/pkg/graveler"
)

type ReadOnlyRepositoriesManager struct {
}

func NewReadOnlyRepositoriesManager() *ReadOnlyRepositoriesManager {
	return &ReadOnlyRepositoriesManager{}
}

func (m *ReadOnlyRepositoriesManager) IsBlocked(repository *graveler.RepositoryRecord) bool {
	return repository.ReadOnly
}
