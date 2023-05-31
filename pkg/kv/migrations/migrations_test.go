package migrations_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-test/deep"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/acl"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/auth/setup"
	authtestutil "github.com/treeverse/lakefs/pkg/auth/testutil"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/kv/migrations"
	"github.com/treeverse/lakefs/pkg/permissions"
	"github.com/treeverse/lakefs/pkg/testutil"
	"golang.org/x/exp/slices"
)

func TestGetMinPermission(t *testing.T) {
	tests := []struct {
		Action     string
		Permission model.ACLPermission
	}{
		{Action: permissions.ReadObjectAction, Permission: acl.ACLRead},
		{Action: "fs:Read*", Permission: acl.ACLRead},
		{Action: "fs:ReadO*", Permission: acl.ACLRead},
		{Action: permissions.ListObjectsAction, Permission: acl.ACLRead},
		{Action: "fs:List*", Permission: acl.ACLRead},
		{Action: permissions.ReadActionsAction, Permission: acl.ACLWrite},
		{Action: "fs:WriteO?ject", Permission: acl.ACLWrite},
	}

	mig := migrations.NewACLsMigrator(nil, false)

	for _, tt := range tests {
		t.Run(tt.Action, func(t *testing.T) {
			permission := mig.GetMinPermission(tt.Action)
			if permission != tt.Permission {
				t.Errorf("Got permission %s != %s", permission, tt.Permission)
			}
		})
	}
}

func TestComputePermission(t *testing.T) {
	tests := []struct {
		Name    string
		Actions []string

		Permission model.ACLPermission
		Err        error
	}{
		{
			Name:       "read-all",
			Actions:    auth.GetActionsForPolicyTypeOrDie("FSRead"),
			Permission: acl.ACLRead,
		}, {
			Name:       "read-one",
			Actions:    []string{permissions.ReadRepositoryAction},
			Permission: acl.ACLRead,
		}, {
			Name:       "read-two",
			Actions:    []string{permissions.ListObjectsAction, permissions.ReadTagAction},
			Permission: acl.ACLRead,
		}, {
			Name:       "only-own-credentials",
			Actions:    auth.GetActionsForPolicyTypeOrDie("AuthManageOwnCredentials"),
			Permission: acl.ACLRead,
		}, {
			Name:       "write-all",
			Actions:    auth.GetActionsForPolicyTypeOrDie("FSReadWrite"),
			Permission: acl.ACLWrite,
		}, {
			Name:       "write-one",
			Actions:    []string{permissions.WriteObjectAction},
			Permission: acl.ACLWrite,
		}, {
			Name:       "write-one-read-one-create-one",
			Actions:    []string{permissions.CreateCommitAction, permissions.ReadObjectAction, permissions.CreateMetaRangeAction},
			Permission: acl.ACLWrite,
		}, {
			Name:       "super-all",
			Actions:    auth.GetActionsForPolicyTypeOrDie("FSFullAccess"),
			Permission: acl.ACLSuper,
		}, {
			Name:       "super-one",
			Actions:    []string{permissions.AttachStorageNamespaceAction},
			Permission: acl.ACLSuper,
		}, {
			Name:       "super-one-write-one-read-two",
			Actions:    []string{permissions.CreateTagAction, permissions.AttachStorageNamespaceAction, permissions.ReadConfigAction, permissions.ReadRepositoryAction},
			Permission: acl.ACLSuper,
		}, {
			Name:       "admin-all",
			Actions:    auth.GetActionsForPolicyTypeOrDie("AllAccess"),
			Permission: acl.ACLAdmin,
		}, {
			Name:       "admin-one",
			Actions:    []string{permissions.SetGarbageCollectionRulesAction},
			Permission: acl.ACLAdmin,
		},
	}

	ctx := context.Background()

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			mig := migrations.NewACLsMigrator(nil, false)

			permission, err := mig.ComputePermission(ctx, tt.Actions)
			if !errors.Is(err, tt.Err) {
				t.Errorf("Got error %s but expected %s", err, tt.Err)
			}
			if permission != tt.Permission {
				t.Errorf("Got permission %s when expecting %s", permission, tt.Permission)
			}
		})
	}
}

func TestBroaderPermission(t *testing.T) {
	perms := []model.ACLPermission{"", acl.ACLRead, acl.ACLWrite, acl.ACLSuper, acl.ACLAdmin}
	for i, a := range perms {
		for j, b := range perms {
			after := i > j
			broader := migrations.BroaderPermission(a, b)
			if after != broader {
				if after {
					t.Error("Expected broader permission")
				} else {
					t.Error("Expected not a broader permission")
				}
			}
		}
	}
}

func getPolicy(t *testing.T, ctx context.Context, svc auth.Service, name string) *model.Policy {
	t.Helper()
	policy, err := svc.GetPolicy(ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	return policy
}

func getPolicies(t *testing.T, ctx context.Context, svc auth.Service, name string) []*model.Policy {
	t.Helper()
	policies, _, err := svc.ListGroupPolicies(ctx, name, &model.PaginationParams{Amount: 1000})
	if err != nil {
		t.Fatal(err)
	}
	return policies
}

func TestNewACLForPolicies_Generator(t *testing.T) {
	now := time.Now()
	ctx := context.Background()
	svc, _ := authtestutil.SetupService(t, ctx, []byte("shh..."))

	err := setup.SetupRBACBaseGroups(ctx, svc, now)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		Name string
		// Policies are passed to NewACLForPolicies
		Policies []*model.Policy
		// ACL is expected to be returned by NewACLForPolicies
		ACL model.ACL
		// Err, if set, is expected to be the error returned from NewACLForPolicies
		Err error
	}{
		{
			Name:     "ExactlyFSFullAccess",
			Policies: []*model.Policy{getPolicy(t, ctx, svc, "FSFullAccess")},
			ACL: model.ACL{
				Permission: acl.ACLSuper,
			},
		}, {
			Name:     "GroupSuperUsers",
			Policies: getPolicies(t, ctx, svc, "SuperUsers"),
			ACL: model.ACL{
				Permission: acl.ACLSuper,
			},
		}, {
			Name:     "ExactlyFSReadAll",
			Policies: []*model.Policy{getPolicy(t, ctx, svc, "FSReadAll")},
			ACL: model.ACL{
				Permission: acl.ACLRead,
			},
		}, {
			Name:     "GroupViewers",
			Policies: getPolicies(t, ctx, svc, "Viewers"),
			ACL: model.ACL{
				Permission: acl.ACLRead,
			},
		},
	}

	mig := migrations.NewACLsMigrator(svc, false)

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			acp, _, err := mig.NewACLForPolicies(ctx, tt.Policies)
			if !errors.Is(err, tt.Err) {
				t.Errorf("Got error %s, expected %s", err, tt.Err)
			}
			if diffs := deep.Equal(acp, &tt.ACL); diffs != nil {
				t.Errorf("Bad ACL: %s", diffs)
			}
		})
	}
}

func TestMigrateImportPermissions(t *testing.T) {
	ctx := context.Background()
	cfg := config.Config{}

	tests := []struct {
		name       string
		policies   []model.Policy
		uiAuthType string
	}{
		{
			name: "eternal_auth_type",
			policies: []model.Policy{
				{
					CreatedAt:   time.Now().Add(-time.Hour).UTC(),
					DisplayName: "import_no_change",
					Statement: []model.Statement{
						{
							Effect:   "allow",
							Action:   []string{permissions.ImportFromStorageAction, permissions.CreatePolicyAction},
							Resource: createARN("importFromStorage"),
						},
					},
				},
			},
			uiAuthType: config.AuthRBACExternal,
		},
		{
			name:       "empty",
			policies:   []model.Policy{},
			uiAuthType: config.AuthRBACSimplified,
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
			uiAuthType: config.AuthRBACSimplified,
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
						},
					},
				},
				{
					CreatedAt:   time.Now().Add(-time.Hour).UTC(),
					DisplayName: "no_import",
					Statement: []model.Statement{
						{
							Effect:   "deny",
							Action:   []string{permissions.RevertBranchAction},
							Resource: createARN("RevertBranchAction"),
						},
					},
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
			uiAuthType: config.AuthRBACSimplified,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authService, store := authtestutil.SetupService(t, ctx, []byte("some secret"))
			for _, policy := range tt.policies {
				testutil.MustDo(t, "create Policy", authService.WritePolicy(ctx, &policy, false))
			}
			// Run migrate
			cfg.Auth.UIConfig.RBAC = tt.uiAuthType
			testutil.MustDo(t, "migrate", migrations.MigrateImportPermissions(ctx, store, &cfg))

			// Verify
			verifyMigration(t, ctx, authService, tt.policies, cfg)
		})
	}
}

func createARN(name string) string {
	return fmt.Sprintf("arn:%s:this:is:an:arn", name)
}
func verifyMigration(t *testing.T, ctx context.Context, authService *auth.AuthService, policies []model.Policy, cfg config.Config) {
	for _, prev := range policies {
		policy, err := authService.GetPolicy(ctx, prev.DisplayName)
		testutil.MustDo(t, "get policy", err)

		require.Equal(t, prev.ACL, policy.ACL)
		if strings.HasPrefix(policy.DisplayName, "import") {
			expected := prev.Statement
			if cfg.IsAuthUISimplified() {
				require.Greater(t, policy.CreatedAt, prev.CreatedAt)
				for _, statement := range expected {
					for {
						idx := slices.Index(statement.Action, permissions.ImportFromStorageAction)
						if idx < 0 {
							break
						}
						statement.Action[idx] = "fs:Import*"
					}
				}
			}
			require.Equal(t, expected, policy.Statement)
		} else {
			require.Equal(t, prev.CreatedAt, policy.CreatedAt)
			require.Equal(t, prev.Statement, policy.Statement)
		}
	}
}
