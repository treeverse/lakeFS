package catalog

import (
	"context"
	"fmt"
	"testing"

	"github.com/treeverse/lakefs/testutil"
)

func TestCataloger_DeleteRepo(t *testing.T) {
	ctx := context.Background()
	cdb, _ := testutil.GetDB(t, databaseURI, "lakefs_catalog")
	c := NewCataloger(cdb)

	// create test data
	for i := 1; i < 3; i++ {
		repoName := fmt.Sprintf("repo%d", i)
		bucketName := fmt.Sprintf("bucket%d", i)
		branchName := fmt.Sprintf("branch%d", i)
		if err := c.CreateRepo(ctx, repoName, bucketName, branchName); err != nil {
			t.Fatal("create repo for testing failed", err)
		}
	}
	tests := []struct {
		name    string
		repo    string
		wantErr bool
	}{
		{
			name:    "existing",
			repo:    "repo2",
			wantErr: false,
		},
		{
			name:    "not found",
			repo:    "repo5",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := c.DeleteRepo(ctx, tt.repo); (err != nil) != tt.wantErr {
				t.Errorf("DeleteRepo() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
