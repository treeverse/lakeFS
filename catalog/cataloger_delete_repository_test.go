package catalog

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/treeverse/lakefs/db"
)

func TestCataloger_DeleteRepository(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)

	// create test data
	for i := 1; i < 3; i++ {
		repoName := fmt.Sprintf("repo%d", i)
		storage := fmt.Sprintf("s3://bucket%d", i)
		branchName := fmt.Sprintf("branch%d", i)
		if err := c.CreateRepository(ctx, repoName, storage, branchName); err != nil {
			t.Fatal("create repository for testing failed", err)
		}
	}
	tests := []struct {
		name       string
		repository string
		wantErr    bool
	}{
		{
			name:       "existing",
			repository: "repo2",
			wantErr:    false,
		},
		{
			name:       "not found",
			repository: "repo5",
			wantErr:    true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := c.DeleteRepository(ctx, tt.repository)
			if (err != nil) != tt.wantErr {
				t.Errorf("DeleteRepository() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil {
				return
			}
			_, err = c.GetRepository(ctx, tt.repository)
			if !errors.Is(err, db.ErrNotFound) {
				t.Errorf("Repository (%s) is not gone: %s", tt.repository, err)
			}
		})
	}
}
