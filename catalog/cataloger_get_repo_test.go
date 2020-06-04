package catalog

import (
	"context"
	"fmt"
	"testing"
)

func TestCataloger_GetRepo(t *testing.T) {
	ctx := context.Background()
	c := setupCatalogerForTesting(t)

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
		name       string
		repo       string
		wantBucket string
		wantBranch string
		wantErr    bool
	}{
		{
			name:       "found",
			repo:       "repo2",
			wantBucket: "bucket2",
			wantBranch: "branch2",
			wantErr:    false,
		},
		{
			name:       "not found",
			repo:       "repo4",
			wantBucket: "bucket4",
			wantBranch: "branch4",
			wantErr:    true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := c.GetRepo(ctx, tt.repo)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetRepo() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil {
				return
			}
			if got.Name != tt.repo {
				t.Errorf("GetRepo() got Name = %v, want %v", got.Name, tt.name)
			}
			if got.DefaultBranch != tt.wantBranch {
				t.Errorf("GetRepo() got DefaultBranch = %v, want %v", got.DefaultBranch, tt.wantBranch)
			}
			if got.StorageNamespace != tt.wantBucket {
				t.Errorf("GetRepo() got StorageNamespace = %v, want %v", got.StorageNamespace, tt.wantBucket)
			}
		})
	}
}
