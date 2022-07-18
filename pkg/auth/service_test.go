package auth_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strconv"
	"testing"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/davecgh/go-spew/spew"
	"github.com/go-test/deep"
	"github.com/google/uuid"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/crypt"
	"github.com/treeverse/lakefs/pkg/auth/model"
	authparams "github.com/treeverse/lakefs/pkg/auth/params"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/permissions"
	"github.com/treeverse/lakefs/pkg/testutil"
)

var (
	pool        *dockertest.Pool
	databaseURI string
	psql        = sq.StatementBuilder.PlaceholderFormat(sq.Dollar)
	someSecret  = []byte("some secret")

	userPoliciesForTesting = []*model.Policy{{
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

func TestMain(m *testing.M) {
	logging.SetLevel("panic")
	var err error
	var closer func()
	pool, err = dockertest.NewPool("")
	if err != nil {
		logging.Default().Fatalf("Could not connect to Docker: %s", err)
	}
	databaseURI, closer = testutil.GetDBInstance(pool)
	code := m.Run()
	closer() // cleanup
	os.Exit(code)
}

func setupKVService(t *testing.T, ctx context.Context) auth.Service {
	t.Helper()
	kvStore := kvtest.GetStore(ctx, t)
	storeMessage := kv.StoreMessage{Store: kvStore}
	return auth.NewKVAuthService(storeMessage, crypt.NewSecretStore(someSecret), nil, authparams.ServiceCache{
		Enabled: false,
	}, logging.Default())
}

func setupDBService(t testing.TB, opts ...testutil.GetDBOption) auth.Service {
	adb, _ := testutil.GetDB(t, databaseURI, opts...)
	return auth.NewDBAuthService(adb, crypt.NewSecretStore(someSecret), nil, authparams.ServiceCache{
		Enabled: false,
	}, logging.Default())
}

func setupService(t *testing.T, ctx context.Context) []DBType {
	tests := []DBType{
		{
			name:        "DB service test",
			authService: setupDBService(t),
		},
		{
			name:        "KV service test",
			authService: setupKVService(t, ctx),
		},
	}
	return tests
}

type DBType struct {
	name        string
	authService auth.Service
}

func userWithPolicies(t testing.TB, s auth.Service, policies []*model.Policy) string {
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
		err := s.WritePolicy(ctx, policy)
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

func TestDBAuthService_ListPaged(t *testing.T) {
	ctx := context.Background()
	const chars = "abcdefghijklmnopqrstuvwxyz"
	adb, _ := testutil.GetDB(t, databaseURI)
	type row struct {
		TheKey string `db:"the_key"`
	}
	if _, err := adb.Exec(ctx, `CREATE TABLE test_pages (the_key text PRIMARY KEY)`); err != nil {
		t.Fatalf("CREATE TABLE test_pages: %s", err)
	}
	insert := psql.Insert("test_pages")
	for _, c := range chars {
		insert = insert.Values(string(c))
	}
	insertSql, args, err := insert.ToSql()
	if err != nil {
		t.Fatalf("create insert statement %v: %s", insert, err)
	}
	if _, err = adb.Exec(ctx, insertSql, args...); err != nil {
		t.Fatalf("%s [%v]: %s", insertSql, args, err)
	}

	for size := 0; size <= len(chars)+1; size++ {
		t.Run(fmt.Sprintf("PageSize%d", size), func(t *testing.T) {
			pagination := &model.PaginationParams{Amount: size}
			if size == 0 { // Overload to mean "don't paginate"
				pagination.Amount = -1
			}
			got := ""
			for {
				values, paginator, err := auth.ListPaged(ctx,
					adb, reflect.TypeOf(row{}), pagination, "the_key", psql.Select("the_key").From("test_pages"))
				if err != nil {
					t.Errorf("ListPaged: %s", err)
					break
				}
				if values == nil {
					t.Fatalf("expected values for pagination %+v but got just paginator %+v", pagination, paginator)
				}
				letters := values.Interface().([]*row)
				for _, c := range letters {
					got = got + c.TheKey
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

func TestKVAuthService_ListPaged(t *testing.T) {
	ctx := context.Background()
	kvStore := kvtest.GetStore(ctx, t)
	storeMessage := kv.StoreMessage{Store: kvStore}
	s := auth.NewKVAuthService(storeMessage, crypt.NewSecretStore(someSecret), nil, authparams.ServiceCache{
		Enabled: false,
	}, logging.Default())

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

func TestDBAuthService_Authorize(t *testing.T) {
	ctx := context.Background()
	tests := setupService(t, ctx)

	cases := []struct {
		name     string
		policies []*model.Policy
		request  func(userName string) *auth.AuthorizationRequest

		expectedAllowed bool
		expectedError   error
	}{
		{
			name: "basic_allowed",
			policies: []*model.Policy{{
				Statement: model.Statements{
					{
						Action:   []string{"fs:WriteObject"},
						Resource: "arn:lakefs:fs:::repository/foo/object/bar",
						Effect:   model.StatementEffectAllow,
					},
				},
			},
			},
			request: func(userName string) *auth.AuthorizationRequest {
				return &auth.AuthorizationRequest{
					Username: userName,
					RequiredPermissions: permissions.Node{
						Permission: permissions.Permission{
							Action:   "fs:WriteObject",
							Resource: "arn:lakefs:fs:::repository/foo/object/bar",
						},
					},
				}
			},
			expectedAllowed: true,
			expectedError:   nil,
		},
		{
			name: "basic_disallowed",
			policies: []*model.Policy{{
				Statement: model.Statements{
					{
						Action:   []string{"fs:WriteObject"},
						Resource: "arn:lakefs:fs:::repository/foo/object/bar",
						Effect:   model.StatementEffectDeny,
					},
				},
			},
			},
			request: func(userName string) *auth.AuthorizationRequest {
				return &auth.AuthorizationRequest{
					Username: userName,
					RequiredPermissions: permissions.Node{
						Permission: permissions.Permission{
							Action:   "fs:WriteObject",
							Resource: "arn:lakefs:fs:::repository/foo/object/bar",
						},
					},
				}
			},
			expectedAllowed: false,
			expectedError:   auth.ErrInsufficientPermissions,
		},
		{
			name: "policy_with_wildcard",
			policies: []*model.Policy{{
				Statement: model.Statements{
					{
						Action:   []string{"fs:WriteObject"},
						Resource: "arn:lakefs:fs:::repository/foo/object/*",
						Effect:   model.StatementEffectAllow,
					},
				},
			},
			},
			request: func(userName string) *auth.AuthorizationRequest {
				return &auth.AuthorizationRequest{
					Username: userName,
					RequiredPermissions: permissions.Node{
						Permission: permissions.Permission{
							Action:   "fs:WriteObject",
							Resource: "arn:lakefs:fs:::repository/foo/object/bar",
						},
					},
				}
			},
			expectedAllowed: true,
			expectedError:   nil,
		},
		{
			name: "policy_with_invalid_user",
			policies: []*model.Policy{{
				Statement: model.Statements{
					{
						Action:   []string{"auth:CreateUser"},
						Resource: "arn:lakefs:auth:::user/${user}",
						Effect:   model.StatementEffectAllow,
					},
				},
			},
			},
			request: func(userName string) *auth.AuthorizationRequest {
				return &auth.AuthorizationRequest{
					Username: userName,
					RequiredPermissions: permissions.Node{
						Permission: permissions.Permission{
							Action:   "auth:CreateUser",
							Resource: "arn:lakefs:auth:::user/foobar",
						},
					},
				}
			},
			expectedAllowed: false,
			expectedError:   auth.ErrInsufficientPermissions,
		},
		{
			name: "policy_with_valid_user",
			policies: []*model.Policy{{
				Statement: model.Statements{
					{
						Action:   []string{"auth:CreateUser"},
						Resource: "arn:lakefs:auth:::user/${user}",
						Effect:   model.StatementEffectAllow,
					},
				},
			}},
			request: func(userName string) *auth.AuthorizationRequest {
				return &auth.AuthorizationRequest{
					Username: userName,
					RequiredPermissions: permissions.Node{
						Permission: permissions.Permission{
							Action:   "auth:CreateUser",
							Resource: fmt.Sprintf("arn:lakefs:auth:::user/%s", userName),
						},
					},
				}
			},
			expectedAllowed: true,
			expectedError:   nil,
		},
		{
			name: "policy_with_other_user",
			policies: []*model.Policy{{
				Statement: model.Statements{
					{
						Action:   []string{"auth:CreateUser"},
						Resource: "arn:lakefs:auth:::user/${user}",
						Effect:   model.StatementEffectAllow,
					},
				},
			},
			},
			request: func(userName string) *auth.AuthorizationRequest {
				return &auth.AuthorizationRequest{
					Username: userName,
					RequiredPermissions: permissions.Node{
						Permission: permissions.Permission{
							Action:   "auth:CreateUser",
							Resource: fmt.Sprintf("arn:lakefs:auth:::user/%sxxxx", userName),
						},
					},
				}
			},
			expectedAllowed: false,
			expectedError:   auth.ErrInsufficientPermissions,
		},
		{
			name: "policy_with_wildcard",
			policies: []*model.Policy{{
				Statement: model.Statements{
					{
						Action:   []string{"auth:CreateUser"},
						Resource: "arn:lakefs:auth:::user/*",
						Effect:   model.StatementEffectAllow,
					},
				},
			},
			},
			request: func(userName string) *auth.AuthorizationRequest {
				return &auth.AuthorizationRequest{
					Username: userName,
					RequiredPermissions: permissions.Node{
						Permission: permissions.Permission{
							Action:   "auth:CreateUser",
							Resource: "arn:lakefs:auth:::user/foobar",
						},
					},
				}
			},
			expectedAllowed: true,
			expectedError:   nil,
		},
		{
			name: "action_passing_wildcards",
			policies: []*model.Policy{{
				Statement: model.Statements{
					{
						Action:   []string{"auth:Create*"},
						Resource: "arn:lakefs:auth:::user/foobar",
						Effect:   model.StatementEffectAllow,
					},
				},
			},
			},
			request: func(userName string) *auth.AuthorizationRequest {
				return &auth.AuthorizationRequest{
					Username: userName,
					RequiredPermissions: permissions.Node{
						Permission: permissions.Permission{
							Action:   "auth:CreateUser",
							Resource: "arn:lakefs:auth:::user/foobar",
						},
					},
				}
			},
			expectedAllowed: true,
			expectedError:   nil,
		},
		{
			name: "action_other_wildcards",
			policies: []*model.Policy{{
				Statement: model.Statements{
					{
						Action:   []string{"auth:Create*"},
						Resource: "arn:lakefs:auth:::user/foobar",
						Effect:   model.StatementEffectAllow,
					},
				},
			},
			},
			request: func(userName string) *auth.AuthorizationRequest {
				return &auth.AuthorizationRequest{
					Username: userName,
					RequiredPermissions: permissions.Node{
						Permission: permissions.Permission{
							Action:   "auth:DeleteUser",
							Resource: "arn:lakefs:auth:::user/foobar",
						},
					},
				}
			},
			expectedAllowed: false,
			expectedError:   auth.ErrInsufficientPermissions,
		},
		{
			name: "action_denying_wildcards",
			policies: []*model.Policy{{
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
			},
			request: func(userName string) *auth.AuthorizationRequest {
				return &auth.AuthorizationRequest{
					Username: userName,
					RequiredPermissions: permissions.Node{
						Permission: permissions.Permission{
							Action:   "auth:DeleteUser",
							Resource: "arn:lakefs:auth:::user/foobar",
						},
					},
				}
			},
			expectedAllowed: false,
			expectedError:   auth.ErrInsufficientPermissions,
		},
	}
	for _, tt := range tests {
		for _, testCase := range cases {
			t.Run(testCase.name, func(t *testing.T) {
				uid := userWithPolicies(t, tt.authService, testCase.policies)
				request := testCase.request(uid)
				response, err := tt.authService.Authorize(ctx, request)
				if err != nil {
					t.Fatal(err)
				}
				if response.Allowed != testCase.expectedAllowed {
					t.Fatalf("expected allowed status %v, got %v", testCase.expectedAllowed, response.Allowed)
				}
				if response.Error != testCase.expectedError {
					t.Fatalf("expected error %v, got %v", testCase.expectedAllowed, response.Error)
				}
			})
		}
	}
}

func TestDBAuthService_ListEffectivePolicies(t *testing.T) {
	ctx := context.Background()
	tests := setupService(t, ctx)

	cases := []struct {
		name             string
		policies         []*model.Policy
		paginationAmount int
		expectedPolicies []string
		expectedError    error
	}{
		{
			name: "effective_policies_with_pagination",
			policies: []*model.Policy{
				{
					DisplayName: "a",
				},
				{
					DisplayName: "b",
				},
				{
					DisplayName: "c",
				},
				{
					DisplayName: "d",
				},
				{
					DisplayName: "e",
				},
				{
					DisplayName: "f",
				},
				{
					DisplayName: "g",
				},
				{
					DisplayName: "h",
				},
			},
			paginationAmount: 3,
			expectedPolicies: []string{"a", "b", "c", "d", "e", "f", "g", "h"},
			expectedError:    nil,
		},
	}
	for _, tt := range tests {
		for _, testCase := range cases {
			t.Run(testCase.name, func(t *testing.T) {
				uid := userWithPolicies(t, tt.authService, testCase.policies)
				pagination := &model.PaginationParams{Amount: testCase.paginationAmount}
				var gotPolicies []string
				for {
					policies, paginator, err := auth.ListEffectivePolicies(ctx, uid, pagination, tt.authService.ListEffectivePolicies, tt.authService.Cache())
					if err != nil {
						t.Errorf("ListEffectivePolicies: %s", err)
						break
					}
					if policies == nil {
						t.Fatalf("expected values for pagination %+v but got just paginator %+v", pagination, paginator)
					}

					for _, p := range policies {
						gotPolicies = append(gotPolicies, p.DisplayName)
					}

					if paginator.NextPageToken == "" {
						if testCase.paginationAmount > 0 && len(policies) > testCase.paginationAmount {
							t.Errorf("expected at most %d entries in last page but got %d", testCase.paginationAmount, len(policies))
						}
						break
					}
					if len(policies) != testCase.paginationAmount {
						t.Errorf("expected %d entries in page but got %d", testCase.paginationAmount, len(policies))
					}
					pagination.After = paginator.NextPageToken
				}
				if len(gotPolicies) != len(testCase.expectedPolicies) {
					t.Fatalf("expected %d entries in page but got %d", len(testCase.expectedPolicies), len(gotPolicies))
				}
				for i := range gotPolicies {
					if testCase.expectedPolicies[i] != gotPolicies[i] {
						t.Errorf("expected %q got %q", testCase.expectedPolicies[i], gotPolicies[i])
					}
				}
			})
		}
	}
}

func TestDBAuthService_ListUsers(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name      string
		userNames []string
	}{
		{
			name:      "no_users",
			userNames: []string{},
		}, {
			name:      "single_user",
			userNames: []string{"foo"},
		}, {
			name:      "many_users",
			userNames: []string{"foo", "bar", "baz", "quux"},
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			tests := setupService(t, ctx)
			for _, tt := range tests {
				for _, userName := range testCase.userNames {
					if _, err := tt.authService.CreateUser(ctx, &model.User{Username: userName}); err != nil {
						t.Fatalf("CreateUser(%s): %s", userName, err)
					}
				}
				gotList, _, err := tt.authService.ListUsers(ctx, &model.PaginationParams{Amount: -1})
				if err != nil {
					t.Fatalf("ListUsers: %s", err)
				}
				gotUsers := make([]string, 0, len(testCase.userNames))
				for _, user := range gotList {
					gotUsers = append(gotUsers, user.Username)
				}
				sort.Strings(gotUsers)
				sort.Strings(testCase.userNames)
				if diffs := deep.Equal(testCase.userNames, gotUsers); diffs != nil {
					t.Errorf("did not get expected user display names: %s", diffs)
				}
			}
		})
	}
}

func TestDBAuthService_ListUserCredentials(t *testing.T) {
	const userName = "accredited"
	ctx := context.Background()
	tests := setupService(t, ctx)
	for _, tt := range tests {
		ctx := context.Background()
		if _, err := tt.authService.CreateUser(ctx, &model.User{Username: userName}); err != nil {
			t.Fatalf("CreateUser(%s): %s", userName, err)
		}
		credential, err := tt.authService.CreateCredentials(ctx, userName)
		if err != nil {
			t.Errorf("CreateCredentials(%s): %s", userName, err)
		}
		credentials, _, err := tt.authService.ListUserCredentials(ctx, userName, &model.PaginationParams{Amount: -1})
		if err != nil {
			t.Errorf("ListUserCredentials(%s): %s", userName, err)
		}
		if len(credentials) != 1 || len(credentials[0].AccessKeyID) == 0 || len(credentials[0].SecretAccessKey) > 0 || len(credentials[0].SecretAccessKeyEncryptedBytes) == 0 {
			t.Errorf("expected to receive single credential with nonempty AccessKeyId and SecretAccessKeyEncryptedBytes and empty SecretAccessKey, got %+v", spew.Sdump(credentials))
		}
		gotCredential := credentials[0]
		if credential.AccessKeyID != gotCredential.AccessKeyID {
			t.Errorf("expected to receive same access key ID %s, got %s", credential.AccessKeyID, gotCredential.AccessKeyID)
		}
		if credential.Username != gotCredential.Username {
			t.Errorf("expected to receive same user ID %s, got %s", credential.Username, gotCredential.Username)
		}
		// Issued dates are somewhat different, make sure not _too_ different.
		timeDiff := credential.IssuedDate.Sub(gotCredential.IssuedDate)
		if timeDiff > time.Second || timeDiff < -1*time.Second {
			t.Errorf("expected to receive issued date close to %s, got %s (diff %s)", credential.IssuedDate, gotCredential.IssuedDate, timeDiff)
		}
	}
	// TODO(ariels): add more credentials (and test)
}

func TestDBAuthService_ListGroups(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name       string
		groupNames []string
	}{
		{
			name:       "no_groups",
			groupNames: []string{},
		}, {
			name:       "single_group",
			groupNames: []string{"fooers"},
		}, {
			name:       "many_groups",
			groupNames: []string{"fooers", "barriers", "bazaars", "quuxers", "pling-plongers"},
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			tests := setupService(t, ctx)
			for _, tt := range tests {
				for _, groupName := range testCase.groupNames {
					if err := tt.authService.CreateGroup(ctx, &model.Group{DisplayName: groupName}); err != nil {
						t.Fatalf("CreateGroup(%s): %s", groupName, err)
					}
				}
				gotGroupNames := make([]string, 0, len(testCase.groupNames))
				groups, _, err := tt.authService.ListGroups(ctx, &model.PaginationParams{Amount: -1})
				if err != nil {
					t.Errorf("ListGroups: %s", err)
				}
				for _, group := range groups {
					gotGroupNames = append(gotGroupNames, group.DisplayName)
				}
				sort.Strings(testCase.groupNames)
				sort.Strings(gotGroupNames)
				if diffs := deep.Equal(testCase.groupNames, gotGroupNames); diffs != nil {
					t.Errorf("got different groups than expected: %s", diffs)
				}
			}
		})
	}

}

func TestDbAuthService_GetUser(t *testing.T) {
	ctx := context.Background()
	tests := setupService(t, ctx)

	const userName = "foo"
	for _, tt := range tests {
		// Time should *not* have nanoseconds - otherwise we are comparing accuracy of golang
		// and Postgres time storage.
		ts := time.Date(2222, 2, 22, 22, 22, 22, 0, time.UTC)
		id, err := tt.authService.CreateUser(ctx, &model.User{Username: userName, CreatedAt: ts})
		if err != nil {
			t.Fatalf("CreateUser(%s): %s", userName, err)
		}
		user, err := tt.authService.GetUser(ctx, userName)
		if err != nil {
			t.Fatalf("GetUser(%s): %s", userName, err)
		}
		if user.Username != userName {
			t.Errorf("GetUser(%s) returned user %+v with a different name", userName, user)
		}
		if user.CreatedAt.Sub(ts) != 0 {
			t.Errorf("expected user CreatedAt %s, got %+v", ts, user.CreatedAt)
		}
		if id == strconv.Itoa(-22) {
			t.Errorf("expected CreateUser ID:-22 to be dropped on server, got user %+v", user)
		}
	}
}

func TestDbAuthService_AddCredentials(t *testing.T) {
	ctx := context.Background()
	const userName = "foo"
	tests := setupService(t, ctx)
	for _, tt := range tests {
		// Time should *not* have nanoseconds - otherwise we are comparing accuracy of golang
		// and Postgres time storage.
		ts := time.Date(2222, 2, 22, 22, 22, 22, 0, time.UTC)
		if _, err := tt.authService.CreateUser(ctx, &model.User{Username: userName, CreatedAt: ts}); err != nil {
			t.Fatalf("CreateUser(%s): %s", userName, err)
		}

		const validKeyID = "AKIAIOSFODNN7EXAMPLE"
		tests := []struct {
			Name      string
			Key       string
			Secret    string
			ExpectErr bool
		}{
			{
				Name:      "empty",
				Key:       "",
				Secret:    "",
				ExpectErr: true,
			},
			{
				Name:      "invalid key",
				Key:       "i",
				Secret:    "secret",
				ExpectErr: true,
			},
			{
				Name:      "invalid secret",
				Key:       validKeyID,
				Secret:    "",
				ExpectErr: true,
			},
			{
				Name:      "valid",
				Key:       validKeyID,
				Secret:    "secret",
				ExpectErr: false,
			},
		}

		for _, test := range tests {
			t.Run(test.Name, func(t *testing.T) {
				_, err := tt.authService.AddCredentials(ctx, userName, test.Key, test.Secret)
				if test.ExpectErr != (err != nil) {
					t.Errorf("AddCredentials with key (%s) expect err=%t, got=%v", test.Key, test.ExpectErr, err)
				}
			})
		}
	}
}

func TestDbAuthService_GetUserById(t *testing.T) {
	ctx := context.Background()
	const userName = "foo"
	tests := setupService(t, ctx)

	for _, tt := range tests {
		// Time should *not* have nanoseconds - otherwise we are comparing accuracy of golang
		// and Postgres time storage.
		ts := time.Date(2222, 2, 22, 22, 22, 22, 0, time.UTC)
		id, err := tt.authService.CreateUser(ctx, &model.User{Username: userName, CreatedAt: ts})
		if err != nil {
			t.Fatalf("CreateUser(%s): %s", userName, err)
		}
		user, err := tt.authService.GetUser(ctx, userName)
		if err != nil {
			t.Fatalf("GetUser(%s): %s", userName, err)
		}

		gotUser, err := tt.authService.GetUserByID(ctx, id)
		if err != nil {
			t.Errorf("GetUserById(%s): %s", id, err)
		}
		if diffs := deep.Equal(user, gotUser); diffs != nil {
			t.Errorf("got different user by name and by ID: %s", diffs)
		}
	}
}

func TestDBAuthService_DeleteUser(t *testing.T) {
	const userName = "foo"
	ctx := context.Background()
	tests := setupService(t, ctx)

	for _, tt := range tests {
		if _, err := tt.authService.CreateUser(ctx, &model.User{Username: userName}); err != nil {
			t.Fatalf("CreateUser(%s): %s", userName, err)
		}
		_, err := tt.authService.GetUser(ctx, userName)
		if err != nil {
			t.Fatalf("GetUser(%s) before deletion: %s", userName, err)
		}
		if err = tt.authService.DeleteUser(ctx, userName); err != nil {
			t.Errorf("DeleteUser(%s): %s", userName, err)
		}
		_, err = tt.authService.GetUser(ctx, userName)
		if err == nil {
			t.Errorf("GetUser(%s) succeeded after DeleteUser", userName)
		} else if !errors.Is(err, auth.ErrNotFound) {
			t.Errorf("GetUser(%s) after deletion: %s", userName, err)
		}
	}
}

func TestAuthService_DeleteUserWithRelations(t *testing.T) {
	userNames := []string{"first", "second"}
	groupNames := []string{"groupA", "groupB"}
	policyNames := []string{"policy01", "policy02", "policy03", "policy04"}

	ctx := context.Background()
	tests := setupService(t, ctx)

	for _, tt := range tests {
		// create initial data set and verify users groups and policies are create and related as expected
		createInitialDataSet(t, ctx, tt.authService, userNames, groupNames, policyNames)
		users, _, err := tt.authService.ListUsers(ctx, &model.PaginationParams{Amount: 100})
		require.NoError(t, err)
		require.NotNil(t, users)
		require.Equal(t, len(userNames), len(users))
		for _, userName := range userNames {
			user, err := tt.authService.GetUser(ctx, userName)
			require.NoError(t, err)
			require.NotNil(t, user)
			require.Equal(t, userName, user.Username)

			groups, _, err := tt.authService.ListUserGroups(ctx, userName, &model.PaginationParams{Amount: 100})
			require.NoError(t, err)
			require.NotNil(t, groups)
			require.Equal(t, len(groupNames), len(groups))

			policies, _, err := tt.authService.ListUserPolicies(ctx, userName, &model.PaginationParams{Amount: 100})
			require.NoError(t, err)
			require.NotNil(t, policies)
			require.Equal(t, len(policyNames)/2, len(policies))

			policies, _, err = tt.authService.ListEffectivePolicies(ctx, userName, &model.PaginationParams{Amount: 100})
			require.NoError(t, err)
			require.NotNil(t, policies)
			require.Equal(t, len(policyNames), len(policies))
		}
		for _, groupName := range groupNames {
			users, _, err := tt.authService.ListGroupUsers(ctx, groupName, &model.PaginationParams{Amount: 100})
			require.NoError(t, err)
			require.NotNil(t, users)
			require.Equal(t, len(userNames), len(users))
		}

		// delete a user
		err = tt.authService.DeleteUser(ctx, userNames[0])
		require.NoError(t, err)

		// verify user does not exist
		user, err := tt.authService.GetUser(ctx, userNames[0])
		require.Error(t, err)
		require.Nil(t, user)

		// verify user is removed from all lists and relations
		users, _, err = tt.authService.ListUsers(ctx, &model.PaginationParams{Amount: 100})
		require.NoError(t, err)
		require.NotNil(t, users)
		require.Equal(t, len(userNames)-1, len(users))

		for _, groupName := range groupNames {
			users, _, err := tt.authService.ListGroupUsers(ctx, groupName, &model.PaginationParams{Amount: 100})
			require.NoError(t, err)
			require.NotNil(t, users)
			require.Equal(t, len(userNames)-1, len(users))
			for _, user := range users {
				require.NotEqual(t, userNames[0], user.Username)
			}
		}
	}
}

func TestAuthService_DeleteGroupWithRelations(t *testing.T) {
	userNames := []string{"first", "second", "third"}
	groupNames := []string{"groupA", "groupB", "groupC"}
	policyNames := []string{"policy01", "policy02", "policy03", "policy04"}

	ctx := context.Background()
	tests := setupService(t, ctx)

	for _, tt := range tests {
		// create initial data set and verify users groups and policies are created and related as expected
		createInitialDataSet(t, ctx, tt.authService, userNames, groupNames, policyNames)
		groups, _, err := tt.authService.ListGroups(ctx, &model.PaginationParams{Amount: 100})
		require.NoError(t, err)
		require.NotNil(t, groups)
		require.Equal(t, len(groupNames), len(groups))
		for _, userName := range userNames {
			user, err := tt.authService.GetUser(ctx, userName)
			require.NoError(t, err)
			require.NotNil(t, user)
			require.Equal(t, userName, user.Username)

			groups, _, err := tt.authService.ListUserGroups(ctx, userName, &model.PaginationParams{Amount: 100})
			require.NoError(t, err)
			require.NotNil(t, groups)
			require.Equal(t, len(groupNames), len(groups))

			policies, _, err := tt.authService.ListUserPolicies(ctx, userName, &model.PaginationParams{Amount: 100})
			require.NoError(t, err)
			require.NotNil(t, policies)
			require.Equal(t, len(policyNames)/2, len(policies))

			policies, _, err = tt.authService.ListEffectivePolicies(ctx, userName, &model.PaginationParams{Amount: 100})
			require.NoError(t, err)
			require.NotNil(t, policies)
			require.Equal(t, len(policyNames), len(policies))
		}
		for _, groupName := range groupNames {
			group, err := tt.authService.GetGroup(ctx, groupName)
			require.NoError(t, err)
			require.NotNil(t, group)
			require.Equal(t, groupName, group.DisplayName)

			users, _, err := tt.authService.ListGroupUsers(ctx, groupName, &model.PaginationParams{Amount: 100})
			require.NoError(t, err)
			require.NotNil(t, users)
			require.Equal(t, len(userNames), len(users))

			policies, _, err := tt.authService.ListGroupPolicies(ctx, groupName, &model.PaginationParams{Amount: 100})
			require.NoError(t, err)
			require.NotNil(t, policies)
			require.Equal(t, len(policyNames)-len(policyNames)/2, len(policies))
		}
		for _, userName := range userNames {
			groups, _, err := tt.authService.ListUserGroups(ctx, userName, &model.PaginationParams{Amount: 100})
			require.NoError(t, err)
			require.NotNil(t, groups)
			require.Equal(t, len(groupNames), len(groups))
		}

		// delete a group
		err = tt.authService.DeleteGroup(ctx, groupNames[1])
		require.NoError(t, err)

		// verify group does not exist
		group, err := tt.authService.GetGroup(ctx, groupNames[1])
		require.Error(t, err)
		require.Nil(t, group)

		// verify group is removed from all lists and relations
		groups, _, err = tt.authService.ListGroups(ctx, &model.PaginationParams{Amount: 100})
		require.NoError(t, err)
		require.NotNil(t, groups)
		require.Equal(t, len(groupNames)-1, len(groups))

		for _, userName := range userNames {
			groups, _, err := tt.authService.ListUserGroups(ctx, userName, &model.PaginationParams{Amount: 100})
			require.NoError(t, err)
			require.NotNil(t, groups)
			require.Equal(t, len(userNames)-1, len(groups))
			for _, group := range groups {
				require.NotEqual(t, groupNames[1], group.DisplayName)
			}
		}
	}
}

func TestAuthService_DeletePoliciesWithRelations(t *testing.T) {
	userNames := []string{"first", "second", "third"}
	groupNames := []string{"groupA", "groupB", "groupC"}
	policyNames := []string{"policy01", "policy02", "policy03", "policy04"}

	ctx := context.Background()
	tests := setupService(t, ctx)

	for _, tt := range tests {
		// create initial data set and verify users groups and policies are create and related as expected
		createInitialDataSet(t, ctx, tt.authService, userNames, groupNames, policyNames)
		policies, _, err := tt.authService.ListPolicies(ctx, &model.PaginationParams{Amount: 100})
		require.NoError(t, err)
		require.NotNil(t, policies)
		require.Equal(t, len(policyNames), len(policies))
		for _, policyName := range policyNames {
			policy, err := tt.authService.GetPolicy(ctx, policyName)
			require.NoError(t, err)
			require.NotNil(t, policy)
			require.Equal(t, policyName, policy.DisplayName)
		}

		for _, groupName := range groupNames {
			policies, _, err := tt.authService.ListGroupPolicies(ctx, groupName, &model.PaginationParams{Amount: 100})
			require.NoError(t, err)
			require.NotNil(t, policies)
			require.Equal(t, len(policyNames)-len(policyNames)/2, len(policies))
		}
		for _, userName := range userNames {
			policies, _, err := tt.authService.ListUserPolicies(ctx, userName, &model.PaginationParams{Amount: 100})
			require.NoError(t, err)
			require.NotNil(t, policies)
			require.Equal(t, len(policyNames)/2, len(policies))

			policies, _, err = tt.authService.ListEffectivePolicies(ctx, userName, &model.PaginationParams{Amount: 100})
			require.NoError(t, err)
			require.NotNil(t, policies)
			require.Equal(t, len(policyNames), len(policies))
		}

		// delete a user policy (beginning of the names list)
		err = tt.authService.DeletePolicy(ctx, policyNames[0])
		require.NoError(t, err)

		// verify policy does not exist
		policy, err := tt.authService.GetPolicy(ctx, policyNames[0])
		require.Error(t, err)
		require.Nil(t, policy)

		// verify policy is removed from all lists and relations
		policies, _, err = tt.authService.ListPolicies(ctx, &model.PaginationParams{Amount: 100})
		require.NoError(t, err)
		require.NotNil(t, policies)
		require.Equal(t, len(policyNames)-1, len(policies))

		for _, userName := range userNames {
			policies, _, err := tt.authService.ListUserPolicies(ctx, userName, &model.PaginationParams{Amount: 100})
			require.NoError(t, err)
			require.NotNil(t, policies)
			require.Equal(t, len(policyNames)/2-1, len(policies))
			for _, policy := range policies {
				require.NotEqual(t, policyNames[0], policy.DisplayName)
			}

			policies, _, err = tt.authService.ListEffectivePolicies(ctx, userName, &model.PaginationParams{Amount: 100})
			require.NoError(t, err)
			require.NotNil(t, policies)
			require.Equal(t, len(policyNames)-1, len(policies))
			for _, policy := range policies {
				require.NotEqual(t, policyNames[0], policy.DisplayName)
			}
		}

		for _, groupName := range groupNames {
			policies, _, err := tt.authService.ListGroupPolicies(ctx, groupName, &model.PaginationParams{Amount: 100})
			require.NoError(t, err)
			require.NotNil(t, policies)
			require.Equal(t, len(policyNames)-len(policyNames)/2, len(policies))
			for _, policy := range policies {
				require.NotEqual(t, policyNames[0], policy.DisplayName)
			}
		}

		// delete a group policy (end of the names list)
		err = tt.authService.DeletePolicy(ctx, policyNames[len(policyNames)-1])
		require.NoError(t, err)

		// verify policy does not exist
		policy, err = tt.authService.GetPolicy(ctx, policyNames[len(policyNames)-1])
		require.Error(t, err)
		require.Nil(t, policy)

		// verify policy is removed from all lists and relations
		policies, _, err = tt.authService.ListPolicies(ctx, &model.PaginationParams{Amount: 100})
		require.NoError(t, err)
		require.NotNil(t, policies)
		require.Equal(t, len(policyNames)-2, len(policies))

		for _, userName := range userNames {
			policies, _, err := tt.authService.ListUserPolicies(ctx, userName, &model.PaginationParams{Amount: 100})
			require.NoError(t, err)
			require.NotNil(t, policies)
			require.Equal(t, len(policyNames)/2-1, len(policies))
			for _, policy := range policies {
				require.NotEqual(t, policyNames[len(policyNames)-1], policy.DisplayName)
			}

			policies, _, err = tt.authService.ListEffectivePolicies(ctx, userName, &model.PaginationParams{Amount: 100})
			require.NoError(t, err)
			require.NotNil(t, policies)
			require.Equal(t, len(policyNames)-2, len(policies))
			for _, policy := range policies {
				require.NotEqual(t, policyNames[len(policyNames)-1], policy.DisplayName)
			}
		}

		for _, groupName := range groupNames {
			policies, _, err := tt.authService.ListGroupPolicies(ctx, groupName, &model.PaginationParams{Amount: 100})
			require.NoError(t, err)
			require.NotNil(t, policies)
			require.Equal(t, len(policyNames)-len(policyNames)/2-1, len(policies))
			for _, policy := range policies {
				require.NotEqual(t, policyNames[len(policyNames)-1], policy.DisplayName)
			}
		}
	}
}

// createInitialDataSet -
// Creates K users with 2 credentials each, L groups and M policies
// Adds all users to all groups
// Attaches M/2 of the policies to all K users and the other M-M/2 policies to all L groups
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
		if err := svc.CreateGroup(ctx, &model.Group{DisplayName: groupName}); err != nil {
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
		if err := svc.WritePolicy(ctx, &model.Policy{DisplayName: policyName, Statement: userPoliciesForTesting[0].Statement}); err != nil {
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

func BenchmarkDBAuthService_ListEffectivePolicies(b *testing.B) {
	// setup user with policies for benchmark
	adb, _ := testutil.GetDB(b, databaseURI)
	serviceWithoutCache := auth.NewDBAuthService(adb, crypt.NewSecretStore(someSecret), nil, authparams.ServiceCache{
		Enabled: false,
	}, logging.Default())
	serviceWithCache := auth.NewDBAuthService(adb, crypt.NewSecretStore(someSecret), nil, authparams.ServiceCache{
		Enabled:        true,
		Size:           1024,
		TTL:            20 * time.Second,
		EvictionJitter: 3 * time.Second,
	}, logging.Default())
	serviceWithCacheLowTTL := auth.NewDBAuthService(adb, crypt.NewSecretStore(someSecret), nil, authparams.ServiceCache{
		Enabled:        true,
		Size:           1024,
		TTL:            1 * time.Millisecond,
		EvictionJitter: 1 * time.Millisecond,
	}, logging.Default())
	userName := userWithPolicies(b, serviceWithoutCache, userPoliciesForTesting)

	b.Run("without_cache", func(b *testing.B) {
		benchmarkDBListEffectivePolicies(b, serviceWithoutCache, userName)
	})
	b.Run("with_cache", func(b *testing.B) {
		benchmarkDBListEffectivePolicies(b, serviceWithCache, userName)
	})
	b.Run("without_cache_low_ttl", func(b *testing.B) {
		benchmarkDBListEffectivePolicies(b, serviceWithCacheLowTTL, userName)
	})
}

func benchmarkDBListEffectivePolicies(b *testing.B, s *auth.DBAuthService, userName string) {
	b.ResetTimer()
	ctx := context.Background()
	for n := 0; n < b.N; n++ {
		_, _, err := s.ListEffectivePolicies(ctx, userName, &model.PaginationParams{Amount: -1})
		if err != nil {
			b.Fatal("Failed to list effective policies", err)
		}
	}
}

func BenchmarkKVAuthService_ListEffectivePolicies(b *testing.B) {
	// setup user with policies for benchmark
	ctx := context.Background()
	kvStore := kvtest.GetStore(ctx, b)
	storeMessage := kv.StoreMessage{Store: kvStore}

	serviceWithoutCache := auth.NewKVAuthService(storeMessage, crypt.NewSecretStore(someSecret), nil, authparams.ServiceCache{
		Enabled: false,
	}, logging.Default())
	serviceWithCache := auth.NewKVAuthService(storeMessage, crypt.NewSecretStore(someSecret), nil, authparams.ServiceCache{
		Enabled:        true,
		Size:           1024,
		TTL:            20 * time.Second,
		EvictionJitter: 3 * time.Second,
	}, logging.Default())
	serviceWithCacheLowTTL := auth.NewKVAuthService(storeMessage, crypt.NewSecretStore(someSecret), nil, authparams.ServiceCache{
		Enabled:        true,
		Size:           1024,
		TTL:            1 * time.Millisecond,
		EvictionJitter: 1 * time.Millisecond,
	}, logging.Default())
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

func benchmarkKVListEffectivePolicies(b *testing.B, s *auth.KVAuthService, userName string) {
	b.ResetTimer()
	ctx := context.Background()
	for n := 0; n < b.N; n++ {
		_, _, err := s.ListEffectivePolicies(ctx, userName, &model.PaginationParams{Amount: -1})
		if err != nil {
			b.Fatal("Failed to list effective policies", err)
		}
	}
}
