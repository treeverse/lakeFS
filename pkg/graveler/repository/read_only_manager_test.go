package repository_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/graveler/repository"
)

var normalRepository = &graveler.RepositoryRecord{
	RepositoryID: "example-repo",
	Repository: &graveler.Repository{
		StorageNamespace: "mem://my-storage",
		DefaultBranchID:  "main",
	},
}

var readOnlyRepository = &graveler.RepositoryRecord{
	RepositoryID: "read-only-repo",
	Repository: &graveler.Repository{
		StorageNamespace: "mem://my-storage",
		DefaultBranchID:  "main",
		ReadOnly:         true,
	},
}

func TestIsBlocked(t *testing.T) {
	tests := map[string]struct {
		repositoryRecord *graveler.RepositoryRecord
	}{
		"normal_repository": {
			repositoryRecord: normalRepository,
		},
		"read_only_repository": {
			repositoryRecord: readOnlyRepository,
		},
	}
	for name, tst := range tests {
		t.Run(name, func(t *testing.T) {
			rorm := repository.NewReadOnlyRepositoriesManager()
			require.Equal(t, tst.repositoryRecord.ReadOnly, rorm.IsBlocked(tst.repositoryRecord))
		})
	}
}
