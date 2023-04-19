package migrate_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-test/deep"

	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/acl"
	"github.com/treeverse/lakefs/pkg/auth/migrate"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/auth/setup"
	"github.com/treeverse/lakefs/pkg/auth/testutil"
	"github.com/treeverse/lakefs/pkg/permissions"
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

	mig := migrate.NewACLsMigrator(nil, false)

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
			Name:       "write-one-read-one",
			Actions:    []string{permissions.CreateCommitAction, permissions.ReadObjectAction},
			Permission: acl.ACLWrite,
		}, {
			Name:       "super-all",
			Actions:    auth.GetActionsForPolicyTypeOrDie("FSFullAccess"),
			Permission: acl.ACLSuper,
		}, {
			Name:       "super-one",
			Actions:    []string{permissions.CreateMetaRangeAction},
			Permission: acl.ACLSuper,
		}, {
			Name:       "super-one-write-one-read-two",
			Actions:    []string{permissions.CreateTagAction, permissions.CreateMetaRangeAction, permissions.ReadStorageConfiguration, permissions.ReadRepositoryAction},
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

	mig := migrate.NewACLsMigrator(nil, false)

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			permission, err := mig.ComputePermission(ctx, tt.Actions)

			if permission != tt.Permission {
				t.Errorf("Got permission %s when expecting %s", permission, tt.Permission)
			}

			if !errors.Is(err, tt.Err) {
				t.Errorf("Got error %s but expected %s", err, tt.Err)
			}
		})
	}
}

func TestBroaderPermission(t *testing.T) {
	perms := []model.ACLPermission{"", acl.ACLRead, acl.ACLWrite, acl.ACLSuper, acl.ACLAdmin}
	for i, a := range perms {
		for j, b := range perms {
			t.Run(fmt.Sprintf("%s:%s", a, b), func(t *testing.T) {
				after := i > j
				broader := migrate.BroaderPermission(a, b)
				if after != broader {
					if after {
						t.Error("Expected broader permission")
					} else {
						t.Error("Expected not a broader permission")
					}
				}
			})
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
	svc := testutil.SetupService(t, ctx, []byte("shh..."))

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
		// Warnings is a slice of warning messages.  Each message
		// should be contained in some returned warning (but not
		// every returned warning must be matched!).
		Warnings []string
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

	mig := migrate.NewACLsMigrator(svc, false)

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			acl, warn, err := mig.NewACLForPolicies(ctx, tt.Policies)
			if !errors.Is(err, tt.Err) {
				t.Errorf("Got error %s, expected %s", err, tt.Err)
			}
			if diffs := deep.Equal(acl, &tt.ACL); diffs != nil {
				t.Errorf("Bad ACL: %s", diffs)
			}
			if len(tt.Warnings) > 0 {
				if warn == nil {
					t.Fatalf("No warnings returned when expecting %v", tt.Warnings)
				}
				fullWarning := warn.Error()
				for _, expectedWarning := range tt.Warnings {
					if !strings.Contains(fullWarning, expectedWarning) {
						t.Errorf("Got warnings %v, which did not include expected warning %s", fullWarning, expectedWarning)
					}
				}
			}
		})
	}
}
