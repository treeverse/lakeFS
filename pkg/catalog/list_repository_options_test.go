package catalog_test

import (
	"testing"

	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/catalog"
)

func TestWithListReposPermissionFilter(t *testing.T) {
	tests := []struct {
		name        string
		username    string
		policies    []*model.Policy
		testRepoIDs []string
		wantVisible []string
	}{
		{
			name:        "no policies filters all",
			username:    "user1",
			policies:    nil,
			testRepoIDs: []string{"repo1", "repo2", "repo3"},
			wantVisible: []string{},
		},
		{
			name:     "wildcard allows all",
			username: "user1",
			policies: []*model.Policy{{
				Statement: model.Statements{{
					Effect:   model.StatementEffectAllow,
					Action:   []string{"fs:*"},
					Resource: "*",
				}},
			}},
			testRepoIDs: []string{"repo1", "repo2", "repo3"},
			wantVisible: []string{"repo1", "repo2", "repo3"},
		},
		{
			name:     "pattern filters correctly",
			username: "user1",
			policies: []*model.Policy{{
				Statement: model.Statements{{
					Effect:   model.StatementEffectAllow,
					Action:   []string{"fs:ListRepositories"},
					Resource: "arn:lakefs:fs:::repository/analytics-*",
				}},
			}},
			testRepoIDs: []string{"analytics-prod", "analytics-dev", "other-repo"},
			wantVisible: []string{"analytics-prod", "analytics-dev"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := catalog.NewListRepositoriesOptions([]catalog.ListRepositoriesOptionsFunc{
				catalog.WithListReposPermissionFilter(tt.username, tt.policies),
			})

			var visible []string
			for _, repoID := range tt.testRepoIDs {
				if opts.FilterFunc(repoID) {
					visible = append(visible, repoID)
				}
			}

			if len(visible) != len(tt.wantVisible) {
				t.Errorf("visible repos count = %d, want %d", len(visible), len(tt.wantVisible))
				return
			}

			for i, v := range visible {
				if v != tt.wantVisible[i] {
					t.Errorf("visible[%d] = %s, want %s", i, v, tt.wantVisible[i])
				}
			}
		})
	}
}
