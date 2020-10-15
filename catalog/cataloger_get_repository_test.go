package catalog

import (
	"context"
	"fmt"
	"testing"
)

func TestCataloger_GetRepository(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t, WithCacheEnabled(false))

	// create test data
	for i := 1; i < 3; i++ {
		repoName := fmt.Sprintf("repo%d", i)
		storage := fmt.Sprintf("s3://bucket%d", i)
		branchName := fmt.Sprintf("branch%d", i)
		if _, err := c.CreateRepository(ctx, repoName, storage, branchName); err != nil {
			t.Fatal("create repository for testing failed", err)
		}
	}

	tests := []struct {
		name                 string
		repository           string
		wantStorageNamespace string
		wantBranch           string
		wantErr              bool
	}{
		{
			name:                 "found",
			repository:           "repo2",
			wantStorageNamespace: "s3://bucket2",
			wantBranch:           "branch2",
			wantErr:              false,
		},
		{
			name:                 "not found",
			repository:           "repo4",
			wantStorageNamespace: "s3://bucket4",
			wantBranch:           "branch4",
			wantErr:              true,
		},
		{
			name:                 "missing repository name",
			repository:           "",
			wantStorageNamespace: "s3://bucket4",
			wantBranch:           "branch4",
			wantErr:              true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := c.GetRepository(ctx, tt.repository)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetRepository() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil {
				return
			}
			if got.Name != tt.repository {
				t.Errorf("GetRepository() got Name = %v, want %v", got.Name, tt.name)
			}
			if got.DefaultBranch != tt.wantBranch {
				t.Errorf("GetRepository() got DefaultBranch = %v, want %v", got.DefaultBranch, tt.wantBranch)
			}
			if got.StorageNamespace != tt.wantStorageNamespace {
				t.Errorf("GetRepository() got StorageNamespace = %v, want %v", got.StorageNamespace, tt.wantStorageNamespace)
			}
		})
	}
}

func BenchmarkCataloger_GetRepository(b *testing.B) {
	ctx := context.Background()
	c := testCataloger(b)
	repo := testCatalogerRepo(b, ctx, c, "repo", "master")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = c.GetRepository(ctx, repo)
	}
}
