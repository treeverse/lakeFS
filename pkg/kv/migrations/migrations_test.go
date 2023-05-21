package migrations_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
	auth_testutil "github.com/treeverse/lakefs/pkg/auth/testutil"
	"github.com/treeverse/lakefs/pkg/kv/migrations"
	"github.com/treeverse/lakefs/pkg/permissions"
	"github.com/treeverse/lakefs/pkg/testutil"
	"golang.org/x/exp/slices"
)

func TestMigrateImportPermissions(t *testing.T) {
	ctx := context.Background()
	authService, store := auth_testutil.SetupService(t, ctx, []byte("some secret"))

	tests := []struct {
		name     string
		policies []model.Policy
	}{
		{
			name:     "empty",
			policies: []model.Policy{},
		},
		{
			name: "no_import",
			policies: []model.Policy{
				{
					CreatedAt:   time.Now().Add(-time.Hour).UTC(),
					DisplayName: "policy1",
					Statement: []model.Statement{
						{
							Effect:   "allow",
							Action:   []string{permissions.CreateBranchAction, permissions.CreatePolicyAction},
							Resource: createARN("policy1"),
						},
					},
				},
				{
					CreatedAt:   time.Now().Add(-time.Hour).UTC(),
					DisplayName: "policy2",
					Statement: []model.Statement{
						{
							Effect:   "deny",
							Action:   []string{permissions.CreateUserAction},
							Resource: createARN("policy2"),
						},
					},
				},
			},
		},
		{
			name: "basic",
			policies: []model.Policy{
				{
					CreatedAt:   time.Now().Add(-time.Hour).UTC(),
					DisplayName: "import1",
					Statement: []model.Statement{
						{
							Effect:   "allow",
							Action:   []string{permissions.ImportFromStorageAction, permissions.CreatePolicyAction},
							Resource: createARN("importFromStorage"),
						}},
				},
				{
					CreatedAt:   time.Now().Add(-time.Hour).UTC(),
					DisplayName: "no_import",
					Statement: []model.Statement{
						{
							Effect:   "deny",
							Action:   []string{permissions.RevertBranchAction},
							Resource: createARN("RevertBranchAction"),
						}},
				},
				{
					CreatedAt:   time.Now().Add(-time.Hour).UTC(),
					DisplayName: "import2",
					Statement: []model.Statement{
						{
							Effect:   "allow",
							Action:   []string{permissions.AddGroupMemberAction, permissions.ImportFromStorageAction, permissions.CreateUserAction},
							Resource: createARN("importFromStorage2"),
						},
						{
							Effect:   "deny",
							Action:   []string{permissions.CreateBranchAction},
							Resource: createARN("CreateBranchAction"),
						},
						{
							Effect:   "deny",
							Action:   []string{permissions.ImportFromStorageAction, permissions.CreateCommitAction},
							Resource: createARN("importFromStorage3"),
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, policy := range tt.policies {
				testutil.MustDo(t, "create Policy", authService.WritePolicy(ctx, &policy, false))
			}
			// Run migrate
			testutil.MustDo(t, "migrate", migrations.MigrateImportPermissions(ctx, store))

			// Verify
			verifyMigration(t, ctx, authService, tt.policies)
		})
	}
}

func createARN(name string) string {
	return fmt.Sprintf("arn:%s:this:is:an:arn", name)
}
func verifyMigration(t *testing.T, ctx context.Context, authService *auth.AuthService, policies []model.Policy) {

	for _, prev := range policies {
		policy, err := authService.GetPolicy(ctx, prev.DisplayName)
		testutil.MustDo(t, "get policy", err)

		require.Equal(t, prev.ACL, policy.ACL)
		if strings.HasPrefix(policy.DisplayName, "import") {
			require.Greater(t, policy.CreatedAt, prev.CreatedAt)
			expected := prev.Statement
			for _, statement := range expected {
				for {
					idx := slices.Index(statement.Action, permissions.ImportFromStorageAction)
					if idx < 0 {
						break
					}
					statement.Action[idx] = "fs:Import*"
				}
			}
			require.Equal(t, expected, policy.Statement)
		} else {
			require.Equal(t, prev.CreatedAt, policy.CreatedAt)
			require.Equal(t, prev.Statement, policy.Statement)
		}
	}
}
