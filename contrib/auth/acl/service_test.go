package acl_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	authacl "github.com/treeverse/lakefs/contrib/auth/acl"
	authtestutil "github.com/treeverse/lakefs/contrib/auth/acl/testutil"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/acl"
	"github.com/treeverse/lakefs/pkg/auth/crypt"
	"github.com/treeverse/lakefs/pkg/auth/model"
	authparams "github.com/treeverse/lakefs/pkg/auth/params"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/permissions"
)

const creationDate = 12345678

var (
	someSecret = []byte("some secret")

	userPoliciesForTesting = []*model.Policy{
		{
			Statement: model.Statements{
				{
					Action:   []string{"auth:DeleteUser"},
					Resource: "arn:lakefs:auth:::user/foobar",
					Effect:   model.StatementEffectAllow,
				},
				{
					Action:   []string{"auth:*"},
					Resource: "*",
					Effect:   model.StatementEffectDeny,
				},
			},
		},
	}
)

func userWithPolicies(t testing.TB, s auth.Service, policies []*model.Policy) string {
	t.Helper()
	ctx := context.Background()
	userName := uuid.New().String()
	_, err := s.CreateUser(ctx, &model.User{
		Username: userName,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, policy := range policies {
		if policy.DisplayName == "" {
			policy.DisplayName = model.CreateID()
		}
		err := s.WritePolicy(ctx, policy, false)
		if err != nil {
			t.Fatal(err)
		}
		err = s.AttachPolicyToUser(ctx, policy.DisplayName, userName)
		if err != nil {
			t.Fatal(err)
		}
	}

	return userName
}

func userWithACLs(t testing.TB, s auth.Service, a model.ACL) string {
	t.Helper()
	statements, err := acl.ACLToStatement(a)
	if err != nil {
		t.Fatal("ACLToStatement: ", err)
	}
	creationTime := time.Unix(creationDate, 0)

	policy := &model.Policy{
		CreatedAt:   creationTime,
		DisplayName: model.CreateID(),
		Statement:   statements,
		ACL:         a,
	}
	return userWithPolicies(t, s, []*model.Policy{policy})
}

// createInitialDataSet -
// Creates K users with 2 credentials each, L groups and M policies
// Add all users to all groups
// Attach M/2 of the policies to all K users and the other M-M/2 policies to all L groups
func createInitialDataSet(t *testing.T, ctx context.Context, svc auth.Service, userNames, groupNames, policyNames []string) {
	for _, userName := range userNames {
		if _, err := svc.CreateUser(ctx, &model.User{Username: userName}); err != nil {
			t.Fatalf("CreateUser(%s): %s", userName, err)
		}
		for i := 0; i < 2; i++ {
			_, err := svc.CreateCredentials(ctx, userName)
			if err != nil {
				t.Errorf("CreateCredentials(%s): %s", userName, err)
			}
		}
	}

	for _, groupName := range groupNames {
		if _, err := svc.CreateGroup(ctx, &model.Group{DisplayName: groupName}); err != nil {
			t.Fatalf("CreateGroup(%s): %s", groupName, err)
		}
		for _, userName := range userNames {
			if err := svc.AddUserToGroup(ctx, userName, groupName); err != nil {
				t.Fatalf("AddUserToGroup(%s, %s): %s", userName, groupName, err)
			}
		}
	}

	numPolicies := len(policyNames)
	for i, policyName := range policyNames {
		if err := svc.WritePolicy(ctx, &model.Policy{DisplayName: policyName, Statement: userPoliciesForTesting[0].Statement}, false); err != nil {
			t.Fatalf("WritePolicy(%s): %s", policyName, err)
		}
		if i < numPolicies/2 {
			for _, userName := range userNames {
				if err := svc.AttachPolicyToUser(ctx, policyName, userName); err != nil {
					t.Fatalf("AttachPolicyToUser(%s, %s): %s", policyName, userName, err)
				}
			}
		} else {
			for _, groupName := range groupNames {
				if err := svc.AttachPolicyToGroup(ctx, policyName, groupName); err != nil {
					t.Fatalf("AttachPolicyToGroup(%s, %s): %s", policyName, groupName, err)
				}
			}
		}
	}
}

func describeAllowed(allowed bool) string {
	if allowed {
		return "allowed"
	}
	return "forbidden"
}

func TestAuthService_ListUsers_PagedWithPrefix(t *testing.T) {
	ctx := context.Background()
	kvStore := kvtest.GetStore(ctx, t)
	s := authacl.NewAuthService(kvStore, crypt.NewSecretStore(someSecret), authparams.ServiceCache{
		Enabled: false,
	}, logging.ContextUnavailable())

	users := []string{"bar", "barn", "baz", "foo", "foobar", "foobaz"}
	for _, u := range users {
		user := model.User{Username: u}
		if _, err := s.CreateUser(ctx, &user); err != nil {
			t.Fatalf("create user: %s", err)
		}
	}

	sizes := []int{10, 3, 2}
	prefixes := []string{"b", "ba", "bar", "f", "foo", "foob", "foobar"}
	for _, size := range sizes {
		for _, p := range prefixes {
			t.Run(fmt.Sprintf("Size:%d;Prefix:%s", size, p), func(t *testing.T) {
				// Only count the correct number of entries were
				// returned; values are tested below.
				got := 0
				after := ""
				for {
					value, paginator, err := s.ListUsers(ctx, &model.PaginationParams{Amount: size, Prefix: p, After: after})
					if err != nil {
						t.Fatal(err)
					}
					got += len(value)
					after = paginator.NextPageToken
					if after == "" {
						break
					}
				}
				// Verify got the right number of users
				count := 0
				for _, u := range users {
					if strings.HasPrefix(u, p) {
						count++
					}
				}
				if got != count {
					t.Errorf("Got %d users when expecting %d", got, count)
				}
			})
		}
	}
}

func TestAuthService_ListPaged(t *testing.T) {
	ctx := context.Background()
	kvStore := kvtest.GetStore(ctx, t)
	s := authacl.NewAuthService(kvStore, crypt.NewSecretStore(someSecret), authparams.ServiceCache{
		Enabled: false,
	}, logging.ContextUnavailable())

	const chars = "abcdefghijklmnopqrstuvwxyz"
	for _, c := range chars {
		user := model.User{Username: string(c)}
		if _, err := s.CreateUser(ctx, &user); err != nil {
			t.Fatalf("create user: %s", err)
		}
	}
	var userData model.UserData

	for size := 0; size <= len(chars)+1; size++ {
		t.Run(fmt.Sprintf("PageSize%d", size), func(t *testing.T) {
			pagination := &model.PaginationParams{Amount: size}
			if size == 0 { // Overload to mean "don't paginate"
				pagination.Amount = -1
			}
			got := ""
			for {
				values, paginator, err := s.ListKVPaged(ctx, (&userData).ProtoReflect().Type(), pagination, model.UserPath(""), false)
				if err != nil {
					t.Errorf("ListPaged: %s", err)
					break
				}
				if values == nil {
					t.Fatalf("expected values for pagination %+v but got just paginator %+v", pagination, paginator)
				}
				letters := model.ConvertUsersDataList(values)
				for _, c := range letters {
					got = got + c.Username
				}
				if paginator.NextPageToken == "" {
					if size > 0 && len(letters) > size {
						t.Errorf("expected at most %d entries in last page but got %d", size, len(letters))
					}
					break
				}
				if len(letters) != size {
					t.Errorf("expected %d entries in page but got %d", size, len(letters))
				}
				pagination.After = paginator.NextPageToken
			}
			if got != chars {
				t.Errorf("Expected to read back \"%s\" but got \"%s\"", chars, got)
			}
		})
	}
}

func BenchmarkKVAuthService_ListEffectivePolicies(b *testing.B) {
	// setup user with policies for benchmark
	ctx := context.Background()
	kvStore := kvtest.GetStore(ctx, b)

	serviceWithoutCache := authacl.NewAuthService(kvStore, crypt.NewSecretStore(someSecret), authparams.ServiceCache{
		Enabled: false,
	}, logging.ContextUnavailable())
	serviceWithCache := authacl.NewAuthService(kvStore, crypt.NewSecretStore(someSecret), authparams.ServiceCache{
		Enabled: true,
		Size:    1024,
		TTL:     20 * time.Second,
		Jitter:  3 * time.Second,
	}, logging.ContextUnavailable())
	serviceWithCacheLowTTL := authacl.NewAuthService(kvStore, crypt.NewSecretStore(someSecret), authparams.ServiceCache{
		Enabled: true,
		Size:    1024,
		TTL:     1 * time.Millisecond,
		Jitter:  1 * time.Millisecond,
	}, logging.ContextUnavailable())
	userName := userWithPolicies(b, serviceWithoutCache, userPoliciesForTesting)

	b.Run("without_cache", func(b *testing.B) {
		benchmarkKVListEffectivePolicies(b, serviceWithoutCache, userName)
	})
	b.Run("with_cache", func(b *testing.B) {
		benchmarkKVListEffectivePolicies(b, serviceWithCache, userName)
	})
	b.Run("without_cache_low_ttl", func(b *testing.B) {
		benchmarkKVListEffectivePolicies(b, serviceWithCacheLowTTL, userName)
	})
}

func benchmarkKVListEffectivePolicies(b *testing.B, s *authacl.AuthService, userName string) {
	b.ResetTimer()
	ctx := context.Background()
	for n := 0; n < b.N; n++ {
		_, _, err := s.ListEffectivePolicies(ctx, userName, &model.PaginationParams{Amount: -1})
		if err != nil {
			b.Fatal("Failed to list effective policies", err)
		}
	}
}

func TestAuthService_DeleteUserWithRelations(t *testing.T) {
	userNames := []string{"first", "second"}
	groupNames := []string{"groupA", "groupB"}
	policyNames := []string{"policy01", "policy02", "policy03", "policy04"}

	ctx := context.Background()
	authService, _ := authtestutil.SetupService(t, ctx, someSecret)

	// create initial data set and verify users groups and policies are created and related as expected
	createInitialDataSet(t, ctx, authService, userNames, groupNames, policyNames)
	users, _, err := authService.ListUsers(ctx, &model.PaginationParams{Amount: 100})
	require.NoError(t, err)
	require.NotNil(t, users)
	require.Equal(t, len(userNames), len(users))
	for _, userName := range userNames {
		user, err := authService.GetUser(ctx, userName)
		require.NoError(t, err)
		require.NotNil(t, user)
		require.Equal(t, userName, user.Username)

		groups, _, err := authService.ListUserGroups(ctx, userName, &model.PaginationParams{Amount: 100})
		require.NoError(t, err)
		require.NotNil(t, groups)
		require.Equal(t, len(groupNames), len(groups))

		policies, _, err := authService.ListUserPolicies(ctx, userName, &model.PaginationParams{Amount: 100})
		require.NoError(t, err)
		require.NotNil(t, policies)
		require.Equal(t, len(policyNames)/2, len(policies))

		policies, _, err = authService.ListEffectivePolicies(ctx, userName, &model.PaginationParams{Amount: 100})
		require.NoError(t, err)
		require.NotNil(t, policies)
		require.Equal(t, len(policyNames), len(policies))
	}
	for _, groupName := range groupNames {
		users, _, err := authService.ListGroupUsers(ctx, groupName, &model.PaginationParams{Amount: 100})
		require.NoError(t, err)
		require.NotNil(t, users)
		require.Equal(t, len(userNames), len(users))
	}

	// delete a user
	err = authService.DeleteUser(ctx, userNames[0])
	require.NoError(t, err)

	// verify user does not exist
	user, err := authService.GetUser(ctx, userNames[0])
	require.Error(t, err)
	require.Nil(t, user)

	// verify user is removed from all lists and relations
	users, _, err = authService.ListUsers(ctx, &model.PaginationParams{Amount: 100})
	require.NoError(t, err)
	require.NotNil(t, users)
	require.Equal(t, len(userNames)-1, len(users))

	for _, groupName := range groupNames {
		users, _, err := authService.ListGroupUsers(ctx, groupName, &model.PaginationParams{Amount: 100})
		require.NoError(t, err)
		require.NotNil(t, users)
		require.Equal(t, len(userNames)-1, len(users))
		for _, user := range users {
			require.NotEqual(t, userNames[0], user.Username)
		}
	}
}

func TestAuthService_DeleteGroupWithRelations(t *testing.T) {
	userNames := []string{"first", "second", "third"}
	groupNames := []string{"groupA", "groupB", "groupC"}
	policyNames := []string{"policy01", "policy02", "policy03", "policy04"}

	ctx := context.Background()
	authService, _ := authtestutil.SetupService(t, ctx, someSecret)

	// create initial data set and verify users groups and policies are created and related as expected
	createInitialDataSet(t, ctx, authService, userNames, groupNames, policyNames)
	groups, _, err := authService.ListGroups(ctx, &model.PaginationParams{Amount: 100})
	require.NoError(t, err)
	require.NotNil(t, groups)
	require.Equal(t, len(groupNames), len(groups))
	for _, userName := range userNames {
		user, err := authService.GetUser(ctx, userName)
		require.NoError(t, err)
		require.NotNil(t, user)
		require.Equal(t, userName, user.Username)

		groups, _, err := authService.ListUserGroups(ctx, userName, &model.PaginationParams{Amount: 100})
		require.NoError(t, err)
		require.NotNil(t, groups)
		require.Equal(t, len(groupNames), len(groups))

		policies, _, err := authService.ListUserPolicies(ctx, userName, &model.PaginationParams{Amount: 100})
		require.NoError(t, err)
		require.NotNil(t, policies)
		require.Equal(t, len(policyNames)/2, len(policies))

		policies, _, err = authService.ListEffectivePolicies(ctx, userName, &model.PaginationParams{Amount: 100})
		require.NoError(t, err)
		require.NotNil(t, policies)
		require.Equal(t, len(policyNames), len(policies))
	}
	for _, groupName := range groupNames {
		group, err := authService.GetGroup(ctx, groupName)
		require.NoError(t, err)
		require.NotNil(t, group)
		require.Equal(t, groupName, group.DisplayName)

		users, _, err := authService.ListGroupUsers(ctx, groupName, &model.PaginationParams{Amount: 100})
		require.NoError(t, err)
		require.NotNil(t, users)
		require.Equal(t, len(userNames), len(users))

		policies, _, err := authService.ListGroupPolicies(ctx, groupName, &model.PaginationParams{Amount: 100})
		require.NoError(t, err)
		require.NotNil(t, policies)
		require.Equal(t, len(policyNames)-len(policyNames)/2, len(policies))
	}
	for _, userName := range userNames {
		groups, _, err := authService.ListUserGroups(ctx, userName, &model.PaginationParams{Amount: 100})
		require.NoError(t, err)
		require.NotNil(t, groups)
		require.Equal(t, len(groupNames), len(groups))
	}

	// delete a group
	err = authService.DeleteGroup(ctx, groupNames[1])
	require.NoError(t, err)

	// verify group does not exist
	group, err := authService.GetGroup(ctx, groupNames[1])
	require.Error(t, err)
	require.Nil(t, group)

	// verify group is removed from all lists and relations
	groups, _, err = authService.ListGroups(ctx, &model.PaginationParams{Amount: 100})
	require.NoError(t, err)
	require.NotNil(t, groups)
	require.Equal(t, len(groupNames)-1, len(groups))

	for _, userName := range userNames {
		groups, _, err := authService.ListUserGroups(ctx, userName, &model.PaginationParams{Amount: 100})
		require.NoError(t, err)
		require.NotNil(t, groups)
		require.Equal(t, len(userNames)-1, len(groups))
		for _, group := range groups {
			require.NotEqual(t, groupNames[1], group.DisplayName)
		}
	}
}

func TestAuthService_DeletePoliciesWithRelations(t *testing.T) {
	userNames := []string{"first", "second", "third"}
	groupNames := []string{"groupA", "groupB", "groupC"}
	policyNames := []string{"policy01", "policy02", "policy03", "policy04"}

	ctx := context.Background()
	authService, _ := authtestutil.SetupService(t, ctx, someSecret)

	// create initial data set and verify users groups and policies are created and related as expected
	createInitialDataSet(t, ctx, authService, userNames, groupNames, policyNames)
	policies, _, err := authService.ListPolicies(ctx, &model.PaginationParams{Amount: 100})
	require.NoError(t, err)
	require.NotNil(t, policies)
	require.Equal(t, len(policyNames), len(policies))
	for _, policyName := range policyNames {
		policy, err := authService.GetPolicy(ctx, policyName)
		require.NoError(t, err)
		require.NotNil(t, policy)
		require.Equal(t, policyName, policy.DisplayName)
	}

	for _, groupName := range groupNames {
		policies, _, err := authService.ListGroupPolicies(ctx, groupName, &model.PaginationParams{Amount: 100})
		require.NoError(t, err)
		require.NotNil(t, policies)
		require.Equal(t, len(policyNames)-len(policyNames)/2, len(policies))
	}
	for _, userName := range userNames {
		policies, _, err := authService.ListUserPolicies(ctx, userName, &model.PaginationParams{Amount: 100})
		require.NoError(t, err)
		require.NotNil(t, policies)
		require.Equal(t, len(policyNames)/2, len(policies))

		policies, _, err = authService.ListEffectivePolicies(ctx, userName, &model.PaginationParams{Amount: 100})
		require.NoError(t, err)
		require.NotNil(t, policies)
		require.Equal(t, len(policyNames), len(policies))
	}

	// delete a user policy (beginning of the name list)
	err = authService.DeletePolicy(ctx, policyNames[0])
	require.NoError(t, err)

	// verify policy does not exist
	policy, err := authService.GetPolicy(ctx, policyNames[0])
	require.Error(t, err)
	require.Nil(t, policy)

	// verify policy is removed from all lists and relations
	policies, _, err = authService.ListPolicies(ctx, &model.PaginationParams{Amount: 100})
	require.NoError(t, err)
	require.NotNil(t, policies)
	require.Equal(t, len(policyNames)-1, len(policies))

	for _, userName := range userNames {
		policies, _, err := authService.ListUserPolicies(ctx, userName, &model.PaginationParams{Amount: 100})
		require.NoError(t, err)
		require.NotNil(t, policies)
		require.Equal(t, len(policyNames)/2-1, len(policies))
		for _, policy := range policies {
			require.NotEqual(t, policyNames[0], policy.DisplayName)
		}

		policies, _, err = authService.ListEffectivePolicies(ctx, userName, &model.PaginationParams{Amount: 100})
		require.NoError(t, err)
		require.NotNil(t, policies)
		require.Equal(t, len(policyNames)-1, len(policies))
		for _, policy := range policies {
			require.NotEqual(t, policyNames[0], policy.DisplayName)
		}
	}

	for _, groupName := range groupNames {
		policies, _, err := authService.ListGroupPolicies(ctx, groupName, &model.PaginationParams{Amount: 100})
		require.NoError(t, err)
		require.NotNil(t, policies)
		require.Equal(t, len(policyNames)-len(policyNames)/2, len(policies))
		for _, policy := range policies {
			require.NotEqual(t, policyNames[0], policy.DisplayName)
		}
	}

	// delete a group policy (end of the names list)
	err = authService.DeletePolicy(ctx, policyNames[len(policyNames)-1])
	require.NoError(t, err)

	// verify policy does not exist
	policy, err = authService.GetPolicy(ctx, policyNames[len(policyNames)-1])
	require.Error(t, err)
	require.Nil(t, policy)

	// verify policy is removed from all lists and relations
	policies, _, err = authService.ListPolicies(ctx, &model.PaginationParams{Amount: 100})
	require.NoError(t, err)
	require.NotNil(t, policies)
	require.Equal(t, len(policyNames)-2, len(policies))

	for _, userName := range userNames {
		policies, _, err := authService.ListUserPolicies(ctx, userName, &model.PaginationParams{Amount: 100})
		require.NoError(t, err)
		require.NotNil(t, policies)
		require.Equal(t, len(policyNames)/2-1, len(policies))
		for _, policy := range policies {
			require.NotEqual(t, policyNames[len(policyNames)-1], policy.DisplayName)
		}

		policies, _, err = authService.ListEffectivePolicies(ctx, userName, &model.PaginationParams{Amount: 100})
		require.NoError(t, err)
		require.NotNil(t, policies)
		require.Equal(t, len(policyNames)-2, len(policies))
		for _, policy := range policies {
			require.NotEqual(t, policyNames[len(policyNames)-1], policy.DisplayName)
		}
	}

	for _, groupName := range groupNames {
		policies, _, err := authService.ListGroupPolicies(ctx, groupName, &model.PaginationParams{Amount: 100})
		require.NoError(t, err)
		require.NotNil(t, policies)
		require.Equal(t, len(policyNames)-len(policyNames)/2-1, len(policies))
		for _, policy := range policies {
			require.NotEqual(t, policyNames[len(policyNames)-1], policy.DisplayName)
		}
	}
}

func TestACL(t *testing.T) {
	hierarchy := []model.ACLPermission{acl.ReadPermission, acl.WritePermission, acl.SuperPermission, acl.AdminPermission}

	type PermissionFrom map[model.ACLPermission][]permissions.Permission
	type TestCase struct {
		// Name is an identifier for this test case.
		Name string
		// ACL is the ACL to test.  ACL.Permission will be tested
		// with each of the hierarchies.
		ACL model.ACL
		// PermissionFrom holds permissions that must hold starting
		// at the ACLPermission key in the hierarchy.
		PermissionFrom PermissionFrom
	}

	tests := []TestCase{
		{
			Name: "all repos",
			ACL:  model.ACL{},
			PermissionFrom: PermissionFrom{
				acl.ReadPermission: []permissions.Permission{
					{Action: permissions.ReadObjectAction, Resource: permissions.ObjectArn("foo", "some/path")},
					{Action: permissions.ListObjectsAction, Resource: permissions.ObjectArn("foo", "some/path")},
					{Action: permissions.ListObjectsAction, Resource: permissions.ObjectArn("quux", "")},
					{Action: permissions.CreateCredentialsAction, Resource: permissions.UserArn("${user}")},
				},
				acl.WritePermission: []permissions.Permission{
					{Action: permissions.WriteObjectAction, Resource: permissions.ObjectArn("foo", "some/path")},
					{Action: permissions.DeleteObjectAction, Resource: permissions.ObjectArn("foo", "some/path")},
					{Action: permissions.CreateBranchAction, Resource: permissions.BranchArn("foo", "twig")},
					{Action: permissions.CreateCommitAction, Resource: permissions.BranchArn("foo", "twig")},
					{Action: permissions.CreateMetaRangeAction, Resource: permissions.RepoArn("foo")},
				},
				acl.SuperPermission: []permissions.Permission{
					{Action: permissions.AttachStorageNamespaceAction, Resource: permissions.StorageNamespace("storage://bucket/path")},
					{Action: permissions.ImportFromStorageAction, Resource: permissions.StorageNamespace("storage://bucket/path")},
					{Action: permissions.ImportCancelAction, Resource: permissions.BranchArn("foo", "twig")},
				},
				acl.AdminPermission: []permissions.Permission{
					{Action: permissions.CreateUserAction, Resource: permissions.UserArn("you")},
				},
			},
		},
	}

	ctx := context.Background()

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			s, _ := authtestutil.SetupService(t, ctx, someSecret)
			userID := make(map[model.ACLPermission]string, len(hierarchy))
			for _, aclPermission := range hierarchy {
				tt.ACL.Permission = aclPermission
				userID[aclPermission] = userWithACLs(t, s, tt.ACL)
			}
			tt.ACL.Permission = ""

			for from, pp := range tt.PermissionFrom {
				for _, p := range pp {
					t.Run(fmt.Sprintf("%+v", p), func(t *testing.T) {
						n := permissions.Node{Permission: p}
						allow := false
						for _, aclPermission := range hierarchy {
							t.Run(string(aclPermission), func(t *testing.T) {
								if aclPermission == from {
									allow = true
								}
								origResource := n.Permission.Resource
								defer func() {
									n.Permission.Resource = origResource
								}()
								n.Permission.Resource = strings.ReplaceAll(n.Permission.Resource, "${user}", userID[aclPermission])

								r, err := s.Authorize(ctx, &auth.AuthorizationRequest{
									Username:            userID[aclPermission],
									RequiredPermissions: n,
								})
								if err != nil {
									t.Errorf("Authorize failed: %v", err)
								}
								if (allow && r.Error != nil) || !allow && !errors.Is(r.Error, auth.ErrInsufficientPermissions) {
									t.Errorf("Authorization response error: %v", err)
								}
								if r.Allowed != allow {
									t.Errorf("%s but expected %s", describeAllowed(r.Allowed), describeAllowed(allow))
								}
							})
						}
					})
				}
			}
		})
	}
}
