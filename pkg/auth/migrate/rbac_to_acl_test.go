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
	"github.com/treeverse/lakefs/pkg/auth/testutil"
	"github.com/treeverse/lakefs/pkg/auth/wildcard"
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
			permission, addedActions, err := mig.ComputePermission(ctx, tt.Actions)

			if permission != tt.Permission {
				t.Errorf("Got permission %s when expecting %s", permission, tt.Permission)
			}

			// Verify added actions.  Getting this correct can
			// be hard and is of lower value to users, because
			// wildcards make life hard.  But we do what we can
			// :-)

			for _, addedAction := range addedActions {
				// Every added action is not covered by the
				// passed-in actions.
				for _, action := range tt.Actions {
					if wildcard.Match(action, addedAction) {
						t.Errorf("Added action %s matches passed-in action %s", addedAction, action)
					}
				}
				// Every added action is covered by the
				// actions for the returned permission.
				ok := false
				for action := range mig.Actions[permission] {
					if wildcard.Match(action, addedAction) {
						ok = true
						break
					}
				}
				if !ok {
					t.Errorf("No action in %v matches added action %s", mig.Actions[permission], addedAction)
				}
			}

			if !errors.Is(err, tt.Err) {
				t.Errorf("Got error %s but expected %s", err, tt.Err)
			}
		})
	}
}

func TestGetRepositories(t *testing.T) {
	tests := []struct {
		Resource     string
		Repositories []string
		All          bool
		Warn         error
	}{
		{Resource: "arn:lakefs:fs:::repository/foo", Repositories: []string{"foo"}},
		{Resource: "arn:lakefs:fs:::repository/*", All: true},
		{Resource: "arn:lakefs:fs:::*", All: true, Warn: migrate.ErrWidened},
		{Resource: "arn:lakefs:fs:::quuxen", All: true, Warn: migrate.ErrWidened},
		{Resource: "arn:lakefs:fs:::repository/dev-*", All: true, Warn: migrate.ErrWidened},
		{Resource: "arn:lakefs:fs:::repository/dev-*/objects/a*", All: true, Warn: migrate.ErrWidened},
		{Resource: "arn:lakefs:fs:::repository/dev-?", All: true, Warn: migrate.ErrWidened},
		{Resource: "arn:lakefs:fs:::repository/foo/objects/", Repositories: []string{"foo"}, Warn: migrate.ErrWidened},
		{Resource: "arn:lakefs:fs:::repository/foo/objects/a*", Repositories: []string{"foo"}, Warn: migrate.ErrWidened},
		{Resource: "arn:lakefs:fs:::repository/foo/objects/a?", Repositories: []string{"foo"}, Warn: migrate.ErrWidened},
		{Resource: "arn:lakefs:fs:::repository/*/objects/", All: true, Warn: migrate.ErrWidened},
		{Resource: "*", All: true},
	}

	mig := migrate.NewACLsMigrator(nil, false)

	for _, tt := range tests {
		t.Run(tt.Resource, func(t *testing.T) {
			repos, all, warn := mig.GetRepositories(tt.Resource)
			if diffs := deep.Equal(repos, tt.Repositories); diffs != nil {
				t.Errorf("Expected different repositories: %s", diffs)
			}
			if tt.All && !all {
				t.Error("Did not get all repositories as expected")
			} else if !tt.All && all {
				t.Error("Got all repositories which was not expected")
			}
			if tt.Warn != nil && !errors.Is(warn, tt.Warn) {
				t.Errorf("Got unexpected warning %s != %s", warn, tt.Warn)
			} else if tt.Warn == nil && warn != nil {
				t.Errorf("Got unexpected warning %s", warn)
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
	ctx := context.Background()
	svc := testutil.SetupService(t, ctx, []byte("shh..."))

	err := auth.SetupBaseGroups(ctx, svc, time.Now())
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
				Permission:   acl.ACLSuper,
				Repositories: model.Repositories{All: true},
			},
		}, {
			Name:     "GroupSuperUsers",
			Policies: getPolicies(t, ctx, svc, "SuperUsers"),
			ACL: model.ACL{
				Permission:   acl.ACLSuper,
				Repositories: model.Repositories{All: true},
			},
		}, {
			Name:     "ExactlyFSReadAll",
			Policies: []*model.Policy{getPolicy(t, ctx, svc, "FSReadAll")},
			ACL: model.ACL{
				Permission:   acl.ACLRead,
				Repositories: model.Repositories{All: true},
			},
		}, {
			Name:     "GroupViewers",
			Policies: getPolicies(t, ctx, svc, "Viewers"),
			ACL: model.ACL{
				Permission:   acl.ACLRead,
				Repositories: model.Repositories{All: true},
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
			fullWarning := warn.Error()
			for _, expectedWarning := range tt.Warnings {
				if !strings.Contains(fullWarning, expectedWarning) {
					t.Errorf("Got warnings %v, which did not include expected warning %s", fullWarning, expectedWarning)
				}
			}
		})
	}
}
