package auth_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"reflect"
	"sort"
	"strconv"
	"testing"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/davecgh/go-spew/spew"
	"github.com/go-openapi/swag"
	"github.com/go-test/deep"
	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/crypt"
	"github.com/treeverse/lakefs/pkg/auth/mock"
	"github.com/treeverse/lakefs/pkg/auth/model"
	authparams "github.com/treeverse/lakefs/pkg/auth/params"
	"github.com/treeverse/lakefs/pkg/db"
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
	storeMessage := &kv.StoreMessage{Store: kvStore}
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
	storeMessage := &kv.StoreMessage{Store: kvStore}
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
				Name:      "invalid_key",
				Key:       "i",
				Secret:    "secret",
				ExpectErr: true,
			},
			{
				Name:      "invalid_secret",
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
	storeMessage := &kv.StoreMessage{Store: kvStore}

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

func TestAPIAuthService_GetUserById(t *testing.T) {
	mockClient, s := NewTestApiService(t, false)
	tests := []struct {
		name               string
		responseStatusCode int
		userID             string
		userIntID          int64
		users              []string
		expectedUserName   string
		expectedErr        error
	}{
		{
			name:               "one_user",
			responseStatusCode: http.StatusOK,
			userID:             "1",
			userIntID:          1,
			users:              []string{"one"},
			expectedUserName:   "one",
			expectedErr:        nil,
		}, {
			name:               "no_users",
			responseStatusCode: http.StatusOK,
			userID:             "2",
			userIntID:          2,
			users:              []string{},
			expectedUserName:   "",
			expectedErr:        db.ErrNotFound,
		},
		{
			name:               "two_responses",
			responseStatusCode: http.StatusOK,
			userID:             "3",
			userIntID:          3,
			users:              []string{"one", "two"},
			expectedUserName:   "",
			expectedErr:        auth.ErrNonUnique,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			returnedUsers := make([]auth.User, len(tt.users))
			for i, u := range tt.users {
				returnedUsers[i] = auth.User{
					Username: u,
				}
			}
			returnedUserList := &auth.UserList{
				Pagination: auth.Pagination{},
				Results:    returnedUsers,
			}

			mockClient.EXPECT().ListUsersWithResponse(gomock.Any(), gomock.Eq(&auth.ListUsersParams{Id: &tt.userIntID})).Return(&auth.ListUsersResponse{
				HTTPResponse: &http.Response{
					StatusCode: tt.responseStatusCode,
				},
				JSON200:     returnedUserList,
				JSON401:     nil,
				JSONDefault: nil,
			}, nil)

			ctx := context.Background()
			gotUser, err := s.GetUserByID(ctx, tt.userID)
			if !errors.Is(err, tt.expectedErr) {
				t.Fatalf("GetUserById(%s): expected err: %v got: %v", tt.userID, tt.expectedErr, err)
			}
			if err != nil {
				return
			}

			if gotUser.Username != tt.expectedUserName {
				t.Fatalf("expected user id:%s, got:%s", tt.expectedUserName, gotUser.Username)
			}
		})
	}
}

func TestAuthAPIUserPoliciesCache(t *testing.T) {
	resPolicies := authPoliciesForTesting
	policyList := auth.PolicyList{
		Pagination: auth.Pagination{},
		Results:    resPolicies,
	}
	mockClient, s := NewTestApiService(t, true)
	response := &auth.ListUserPoliciesResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusOK},
		JSON200:      &policyList,
	}
	const username = "username"
	mockClient.EXPECT().ListUserPoliciesWithResponse(gomock.Any(), username, gomock.Any()).Return(response, nil)
	res1, _, err := s.ListEffectivePolicies(context.Background(), username, &model.PaginationParams{Amount: -1})
	testutil.Must(t, err)
	res2, _, err := s.ListEffectivePolicies(context.Background(), username, &model.PaginationParams{Amount: -1})
	testutil.Must(t, err)
	if diff := deep.Equal(res1, res2); diff != nil {
		t.Error("cache returned different result than api", diff)
	}
}

func TestAuthApiGetCredentialsCache(t *testing.T) {
	ctx := context.Background()
	mockClient, s := NewTestApiService(t, true)
	const username = "foo"
	accessKey := "ACCESS"
	secretKey := "SECRET"
	response := &auth.GetCredentialsResponse{
		HTTPResponse: &http.Response{
			StatusCode: http.StatusOK,
		},
		JSON200: &auth.CredentialsWithSecret{
			AccessKeyId:     accessKey,
			SecretAccessKey: secretKey,
		},
	}
	mockClient.EXPECT().GetCredentialsWithResponse(gomock.Any(), gomock.Any()).Return(response, nil)
	mockClient.EXPECT().ListUsersWithResponse(gomock.Any(), gomock.Any(), gomock.Any()).Return(&auth.ListUsersResponse{
		HTTPResponse: &http.Response{
			StatusCode: http.StatusOK,
		},
		JSON200: &auth.UserList{
			Pagination: auth.Pagination{},
			Results: []auth.User{{
				Username: username,
			}},
		},
	}, nil)

	res1, err := s.GetCredentials(ctx, accessKey)
	testutil.Must(t, err)
	res2, err := s.GetCredentials(ctx, accessKey)
	testutil.Must(t, err)
	if diff := deep.Equal(res1, res2); diff != nil {
		t.Error("cache returned different result than api", diff)
	}
}

func TestAuthApiGetUserCache(t *testing.T) {
	ctx := context.Background()
	mockClient, s := NewTestApiService(t, true)
	const userID = "123"
	const uid = int64(123)

	const username = "foo"
	userMail := "foo@test.com"
	externalId := "1234"
	userResult := auth.User{
		Username:   username,
		Id:         uid,
		Email:      &userMail,
		ExternalId: &externalId,
	}

	returnedUserList := &auth.UserList{
		Pagination: auth.Pagination{},
		Results: []auth.User{
			userResult,
		},
	}
	t.Run("get_user_by_id", func(t *testing.T) {
		mockClient.EXPECT().ListUsersWithResponse(gomock.Any(), gomock.Any()).Return(&auth.ListUsersResponse{
			HTTPResponse: &http.Response{
				StatusCode: http.StatusOK,
			},
			JSON200: returnedUserList,
		}, nil)

		res1, err := s.GetUserByID(ctx, userID)
		testutil.Must(t, err)
		// call again and check
		res2, err := s.GetUserByID(ctx, userID)
		testutil.Must(t, err)
		if diff := deep.Equal(res1, res2); diff != nil {
			t.Error("cache returned different result than api", diff)
		}
	})
	t.Run("get_user_by_email", func(t *testing.T) {
		mockClient.EXPECT().ListUsersWithResponse(gomock.Any(), gomock.Any()).Return(&auth.ListUsersResponse{
			HTTPResponse: &http.Response{
				StatusCode: http.StatusOK,
			},
			JSON200: returnedUserList,
		}, nil)
		res1, err := s.GetUserByEmail(ctx, userMail)
		testutil.Must(t, err)
		// call again and check
		res2, err := s.GetUserByEmail(ctx, userMail)
		testutil.Must(t, err)
		if diff := deep.Equal(res1, res2); diff != nil {
			t.Error("cache returned different result than api", diff)
		}
	})
	t.Run("get_user", func(t *testing.T) {
		mockClient.EXPECT().GetUserWithResponse(gomock.Any(), gomock.Any()).Return(&auth.GetUserResponse{
			HTTPResponse: &http.Response{
				StatusCode: http.StatusOK,
			},
			JSON200: &userResult,
		}, nil)
		res1, err := s.GetUser(ctx, username)
		testutil.Must(t, err)
		// call again and check
		res2, err := s.GetUser(ctx, username)
		testutil.Must(t, err)
		if diff := deep.Equal(res1, res2); diff != nil {
			t.Error("cache returned different result than api", diff)
		}
	})
	t.Run("get_user", func(t *testing.T) {
		mockClient.EXPECT().ListUsersWithResponse(gomock.Any(), gomock.Any()).Return(&auth.ListUsersResponse{
			HTTPResponse: &http.Response{
				StatusCode: http.StatusOK,
			},
			JSON200: returnedUserList,
		}, nil)
		res1, err := s.GetUserByExternalID(ctx, externalId)
		testutil.Must(t, err)
		// call again and check
		res2, err := s.GetUserByExternalID(ctx, externalId)
		testutil.Must(t, err)
		if diff := deep.Equal(res1, res2); diff != nil {
			t.Error("cache returned different result than api", diff)
		}
	})
}

func TestAPIAuthService_CreateUser(t *testing.T) {
	mockClient, s := NewTestApiService(t, false)
	tests := []struct {
		name               string
		userName           string
		email              string
		friendlyName       string
		source             string
		responseStatusCode int
		responseID         int64
		expectedResponseID string
		expectedErr        error
	}{
		{
			name:               "successful",
			userName:           "foo",
			email:              "foo@gmail.com",
			friendlyName:       "friendly foo",
			source:             "internal",
			responseID:         1,
			responseStatusCode: http.StatusCreated,
			expectedResponseID: "1",
			expectedErr:        nil,
		},
		{
			name:               "invalid_user",
			userName:           "",
			email:              "foo@gmail.com",
			friendlyName:       "friendly foo",
			source:             "internal",
			responseStatusCode: http.StatusBadRequest,
			expectedResponseID: auth.InvalidUserID,
			expectedErr:        auth.ErrInvalidRequest,
		},
		{
			name:               "user_exists",
			userName:           "existingUser",
			email:              "foo@gmail.com",
			friendlyName:       "friendly foo",
			source:             "internal",
			responseStatusCode: http.StatusConflict,
			expectedResponseID: auth.InvalidUserID,
			expectedErr:        auth.ErrAlreadyExists,
		},
		{
			name:               "internal_error",
			userName:           "user",
			email:              "foo@gmail.com",
			source:             "internal",
			responseStatusCode: http.StatusInternalServerError,
			expectedResponseID: auth.InvalidUserID,
			expectedErr:        auth.ErrUnexpectedStatusCode,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			response := &auth.CreateUserResponse{
				HTTPResponse: &http.Response{
					StatusCode: tt.responseStatusCode,
				},
				JSON201: &auth.User{
					Id: tt.responseID,
				},
			}
			mockClient.EXPECT().CreateUserWithResponse(gomock.Any(), auth.CreateUserJSONRequestBody{
				Email:        &tt.email,
				FriendlyName: &tt.friendlyName,
				Source:       &tt.source,
				Username:     tt.userName,
			}).Return(response, nil)
			ctx := context.Background()
			res, err := s.CreateUser(ctx, &model.User{
				Username:     tt.userName,
				FriendlyName: &tt.friendlyName,
				Email:        &tt.email,
				Source:       tt.source,
			})
			if !errors.Is(err, tt.expectedErr) {
				t.Fatalf("CreateUser: expected err: %v got: %v", tt.expectedErr, err)
			}
			if res != tt.expectedResponseID {
				t.Fatalf("CreateUser: expected user.id: %s got: %s", tt.expectedResponseID, res)
			}
		})
	}
}

func TestAPIAuthService_DeleteUser(t *testing.T) {
	mockClient, s := NewTestApiService(t, false)
	tests := []struct {
		name               string
		userName           string
		responseStatusCode int
		expectedErr        error
	}{
		{
			name:               "successful",
			userName:           "foo",
			responseStatusCode: http.StatusNoContent,
			expectedErr:        nil,
		},
		{
			name:               "non_existing_user",
			userName:           "nobody",
			responseStatusCode: http.StatusNotFound,
			expectedErr:        auth.ErrNotFound, // TODO(Guys): change this once we change this to the right error
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			response := &auth.DeleteUserResponse{
				HTTPResponse: &http.Response{
					StatusCode: tt.responseStatusCode,
				},
			}
			ctx := context.Background()
			mockClient.EXPECT().DeleteUserWithResponse(ctx, tt.userName).Return(response, nil)
			err := s.DeleteUser(ctx, tt.userName)
			if !errors.Is(err, tt.expectedErr) {
				t.Fatalf("DeleteUser: expected err: %v got: %v", tt.expectedErr, err)
			}
		})
	}
}

func TestAPIAuthService_GetUserByEmail(t *testing.T) {
	mockClient, s := NewTestApiService(t, false)

	tests := []struct {
		name               string
		responseStatusCode int
		users              []string
		email              string
		expectedUserName   string
		expectedErr        error
	}{
		{
			name:               "one_user",
			responseStatusCode: http.StatusOK,
			users:              []string{"one"},
			email:              "one@test.com",
			expectedUserName:   "one",
			expectedErr:        nil,
		}, {
			name:               "no_users",
			responseStatusCode: http.StatusOK,
			users:              []string{},
			email:              "noone@test.com",
			expectedUserName:   "",
			expectedErr:        db.ErrNotFound,
		},
		{
			name:               "two_responses",
			responseStatusCode: http.StatusOK,
			email:              "both@test.com",
			users:              []string{"one", "two"},
			expectedUserName:   "",
			expectedErr:        auth.ErrNonUnique,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			returnedUsers := make([]auth.User, len(tt.users))
			for i, u := range tt.users {
				returnedUsers[i] = auth.User{
					Username: u,
					Email:    &tt.email,
				}
			}
			returnedUserList := &auth.UserList{
				Pagination: auth.Pagination{},
				Results:    returnedUsers,
			}
			mockClient.EXPECT().ListUsersWithResponse(gomock.Any(), gomock.Eq(&auth.ListUsersParams{Email: swag.String(tt.email)})).Return(&auth.ListUsersResponse{
				Body: nil,
				HTTPResponse: &http.Response{
					StatusCode: tt.responseStatusCode,
				},
				JSON200:     returnedUserList,
				JSON401:     nil,
				JSONDefault: nil,
			}, nil)

			ctx := context.Background()
			gotUser, err := s.GetUserByEmail(ctx, tt.email)
			if !errors.Is(err, tt.expectedErr) {
				t.Fatalf("GetUserByEmail(%s): expected err: %v got: %v", tt.email, tt.expectedErr, err)
			}
			if err != nil {
				return
			}

			if gotUser.Username != tt.expectedUserName {
				t.Fatalf("expected username:%s, got:%s", tt.expectedUserName, gotUser.Username)
			}
		})
	}

}

func NewTestApiService(t *testing.T, withCache bool) (*mock.MockClientWithResponsesInterface, *auth.APIAuthService) {
	t.Helper()
	ctrl := gomock.NewController(t)
	mockClient := mock.NewMockClientWithResponsesInterface(ctrl)
	secretStore := crypt.NewSecretStore([]byte("secret"))
	cacheParams := authparams.ServiceCache{}
	if withCache {
		cacheParams.Enabled = true
		cacheParams.Size = 100
		cacheParams.TTL = time.Minute
		cacheParams.EvictionJitter = time.Minute
	}
	s, err := auth.NewAPIAuthServiceWithClient(mockClient, secretStore, cacheParams)
	if err != nil {
		t.Fatalf("failed initiating API service with mock")
	}
	return mockClient, s
}

func TestAPIAuthService_GetUser(t *testing.T) {
	mockClient, s := NewTestApiService(t, false)
	tests := []struct {
		name               string
		userName           string
		email              string
		friendlyName       string
		source             string
		encryptedPassword  []byte
		responseStatusCode int
		responseName       string
		expectedResponseID string
		expectedErr        error
	}{
		{
			name:               "successful",
			userName:           "foo",
			email:              "foo@gmail.com",
			encryptedPassword:  []byte("password"),
			friendlyName:       "friendly foo",
			source:             "internal",
			responseName:       "foo",
			responseStatusCode: http.StatusOK,
			expectedErr:        nil,
		},
		{
			name:               "invalid_user",
			userName:           "",
			email:              "",
			encryptedPassword:  nil,
			friendlyName:       "",
			source:             "",
			responseStatusCode: http.StatusBadRequest,
			expectedErr:        auth.ErrInvalidRequest,
		},
		{
			name:               "internal_error",
			userName:           "user",
			email:              "",
			encryptedPassword:  nil,
			source:             "",
			responseStatusCode: http.StatusInternalServerError,
			expectedErr:        auth.ErrUnexpectedStatusCode,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			response := &auth.GetUserResponse{
				HTTPResponse: &http.Response{
					StatusCode: tt.responseStatusCode,
				},
				JSON200: &auth.User{
					Username:          tt.responseName,
					Email:             &tt.email,
					FriendlyName:      &tt.friendlyName,
					Source:            &tt.source,
					EncryptedPassword: tt.encryptedPassword,
				},
			}
			mockClient.EXPECT().GetUserWithResponse(gomock.Any(), tt.userName).Return(response, nil)
			ctx := context.Background()
			user, err := s.GetUser(ctx, tt.userName)
			if !errors.Is(err, tt.expectedErr) {
				t.Fatalf("GetUser: expected err: %v got: %v", tt.expectedErr, err)
			}
			if err != nil {
				return
			}
			if user.Username != tt.responseName {
				t.Errorf("expected response user name:%s, got:%s", tt.responseName, user.Username)
			}
			if swag.StringValue(user.Email) != tt.email {
				t.Errorf("expected response email :%s, got:%s", tt.responseName, swag.StringValue(user.Email))
			}
			if swag.StringValue(user.FriendlyName) != tt.friendlyName {
				t.Errorf("expected response friendly name :%s, got:%s", tt.responseName, swag.StringValue(user.FriendlyName))
			}
			if user.Source != tt.source {
				t.Errorf("expected response source :%s, got:%s", tt.responseName, user.Source)
			}
			if !bytes.Equal(user.EncryptedPassword, tt.encryptedPassword) {
				t.Errorf("expected response password :%s, got:%s", tt.encryptedPassword, user.EncryptedPassword)
			}
		})
	}
}

func TestAPIAuthService_GetGroup(t *testing.T) {
	mockClient, s := NewTestApiService(t, false)
	tests := []struct {
		name               string
		groupName          string
		responseStatusCode int
		responseName       string
		expectedErr        error
	}{
		{
			name:               "successful",
			groupName:          "foo",
			responseName:       "foo",
			responseStatusCode: http.StatusOK,
			expectedErr:        nil,
		},
		{
			name:               "invalid_group",
			groupName:          "",
			responseStatusCode: http.StatusBadRequest,
			expectedErr:        auth.ErrInvalidRequest,
		},
		{
			name:        "internal_error",
			groupName:   "group",
			expectedErr: auth.ErrUnexpectedStatusCode,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			response := &auth.GetGroupResponse{
				HTTPResponse: &http.Response{
					StatusCode: tt.responseStatusCode,
				},
				JSON200: &auth.Group{
					Name: tt.responseName,
				},
			}
			mockClient.EXPECT().GetGroupWithResponse(gomock.Any(), tt.groupName).Return(response, nil)
			ctx := context.Background()
			group, err := s.GetGroup(ctx, tt.groupName)
			if !errors.Is(err, tt.expectedErr) {
				t.Fatalf("GetGroup: expected err: %v got: %v", tt.expectedErr, err)
			}
			if err != nil {
				return
			}
			if group.DisplayName != tt.responseName {
				t.Errorf("expected response group name:%s, got:%s", tt.responseName, group.DisplayName)
			}
		})
	}
}

func TestAPIAuthService_GetCredentials(t *testing.T) {
	mockClient, s := NewTestApiService(t, false)
	tests := []struct {
		name               string
		responseStatusCode int
		reqAccessKey       string
		accessKey          string
		secretKey          string
		username           string
		expectedErr        error
	}{
		{
			name:               "successful",
			reqAccessKey:       "AKIA",
			accessKey:          "AKIA",
			secretKey:          "SECRET",
			username:           "foo",
			responseStatusCode: http.StatusOK,
			expectedErr:        nil,
		},
		{
			name:               "invalid_credentials",
			reqAccessKey:       "AKIA",
			responseStatusCode: http.StatusBadRequest,
			expectedErr:        auth.ErrInvalidRequest,
		},
		{
			name:         "internal_error",
			reqAccessKey: "AKIA",
			expectedErr:  auth.ErrUnexpectedStatusCode,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			response := &auth.GetCredentialsResponse{
				HTTPResponse: &http.Response{
					StatusCode: tt.responseStatusCode,
				},
				JSON200: &auth.CredentialsWithSecret{
					AccessKeyId:     tt.accessKey,
					SecretAccessKey: tt.secretKey,
				},
			}
			mockClient.EXPECT().GetCredentialsWithResponse(gomock.Any(), tt.accessKey).Return(response, nil)
			mockClient.EXPECT().ListUsersWithResponse(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(&auth.ListUsersResponse{
				HTTPResponse: &http.Response{
					StatusCode: http.StatusOK,
				},
				JSON200: &auth.UserList{
					Pagination: auth.Pagination{},
					Results: []auth.User{{
						Username: tt.username,
					}},
				},
			}, nil)
			ctx := context.Background()
			credentials, err := s.GetCredentials(ctx, tt.accessKey)
			if !errors.Is(err, tt.expectedErr) {
				t.Fatalf("GetCredentials: expected err: %v got: %v", tt.expectedErr, err)
			}
			if err != nil {
				return
			}
			if credentials.AccessKeyID != tt.accessKey {
				t.Errorf("expected response accessKeyID:%s, got:%s", tt.accessKey, credentials.AccessKeyID)
			}
			if credentials.SecretAccessKey != tt.secretKey {
				t.Errorf("expected response SecretAccessKey:%s, got:%s", tt.accessKey, credentials.SecretAccessKey)
			}
			if credentials.Username != tt.username {
				t.Errorf("expected response username:%s, got:%s", tt.username, credentials.Username)
			}
		})
	}
}

func TestAPIAuthService_GetCredentialsForUser(t *testing.T) {
	mockClient, s := NewTestApiService(t, false)
	tests := []struct {
		name               string
		responseStatusCode int
		reqAccessKey       string
		accessKey          string
		secretKey          string
		username           string
		expectedErr        error
	}{
		{
			name:               "successful",
			reqAccessKey:       "AKIA",
			accessKey:          "AKIA",
			secretKey:          "SECRET",
			username:           "foo",
			responseStatusCode: http.StatusOK,
			expectedErr:        nil,
		},
		{
			name:               "invalid_credentials",
			reqAccessKey:       "AKIA",
			responseStatusCode: http.StatusBadRequest,
			expectedErr:        auth.ErrInvalidRequest,
		},
		{
			name:         "internal_error",
			reqAccessKey: "AKIA",
			expectedErr:  auth.ErrUnexpectedStatusCode,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			response := &auth.GetCredentialsForUserResponse{
				HTTPResponse: &http.Response{
					StatusCode: tt.responseStatusCode,
				},
				JSON200: &auth.Credentials{
					AccessKeyId: tt.accessKey,
				},
			}
			mockClient.EXPECT().GetCredentialsForUserWithResponse(gomock.Any(), tt.username, tt.accessKey).Return(response, nil)
			ctx := context.Background()
			credentials, err := s.GetCredentialsForUser(ctx, tt.username, tt.accessKey)
			if !errors.Is(err, tt.expectedErr) {
				t.Fatalf("GetCredentialsForUser: expected err: %v got: %v", tt.expectedErr, err)
			}
			if err != nil {
				return
			}
			if credentials.AccessKeyID != tt.accessKey {
				t.Errorf("expected response accessKeyID:%s, got:%s", tt.accessKey, credentials.AccessKeyID)
			}
			if credentials.Username != tt.username {
				t.Errorf("expected response username:%s, got:%s", tt.username, credentials.Username)
			}
		})
	}
}

func TestAPIAuthService_ListGroups(t *testing.T) {
	mockClient, s := NewTestApiService(t, false)
	const groupNamePrefix = "groupNamePrefix"
	const creationDate = 12345678
	amounts := []int{0, 1, 5}
	for _, amount := range amounts {
		t.Run(fmt.Sprintf("amount_%d", amount), func(t *testing.T) {
			groups := make([]auth.Group, amount)
			for i := 0; i < amount; i++ {
				groups[i] = auth.Group{
					CreationDate: creationDate,
					Name:         fmt.Sprintf("%s-%d", groupNamePrefix, i),
				}
			}
			groupList := auth.GroupList{
				Pagination: auth.Pagination{},
				Results:    groups,
			}
			response := &auth.ListGroupsResponse{
				HTTPResponse: &http.Response{StatusCode: http.StatusOK},
			}
			response.JSON200 = &groupList
			paginationParams := model.PaginationParams{}
			paginationPrefix := auth.PaginationPrefix(paginationParams.Prefix)
			paginationAfter := auth.PaginationAfter(paginationParams.After)
			paginationAmount := auth.PaginationAmount(paginationParams.Amount)
			mockClient.EXPECT().ListGroupsWithResponse(gomock.Any(), gomock.Eq(&auth.ListGroupsParams{
				Prefix: &paginationPrefix,
				After:  &paginationAfter,
				Amount: &paginationAmount,
			})).Return(response, nil)
			group, _, err := s.ListGroups(context.Background(), &paginationParams)
			if err != nil {
				t.Fatalf("failed with error - %s", err)
			}

			creationTime := time.Unix(creationDate, 0)
			for i, g := range group {
				if g == nil {
					t.Fatalf("got nil group")
				}
				expected := fmt.Sprintf("%s-%d", groupNamePrefix, i)
				if g.DisplayName != expected {
					t.Errorf("ListGroups item %d, expected displayName:%s got:%s", i, g.DisplayName, expected)
				}
				if !g.CreatedAt.Equal(creationTime) {
					t.Errorf("eListGroups item %d, expected created date:%s got:%s for %s", i, g.CreatedAt, creationTime, expected)
				}
			}
		})
	}
}

func TestAPIAuthService_ListUsers(t *testing.T) {
	mockClient, s := NewTestApiService(t, false)
	const userNamePrefix = "userNamePrefix"
	const creationDate = 12345678
	amounts := []int{0, 1, 5}
	for _, amount := range amounts {
		t.Run(fmt.Sprintf("amount_%d", amount), func(t *testing.T) {
			users := make([]auth.User, amount)
			for i := 0; i < amount; i++ {
				users[i] = auth.User{
					CreationDate: creationDate,
					Username:     fmt.Sprintf("%s-%d", userNamePrefix, i),
				}
			}
			userList := auth.UserList{
				Pagination: auth.Pagination{},
				Results:    users,
			}
			response := &auth.ListUsersResponse{
				HTTPResponse: &http.Response{StatusCode: http.StatusOK},
			}
			response.JSON200 = &userList
			mockClient.EXPECT().ListUsersWithResponse(gomock.Any(), gomock.Any()).Return(response, nil)
			user, _, err := s.ListUsers(context.Background(), &model.PaginationParams{})
			if err != nil {
				t.Fatalf("failed with error - %s", err)
			}

			creationTime := time.Unix(creationDate, 0)
			for i, g := range user {
				if g == nil {
					t.Fatalf("got nil user")
				}
				expected := fmt.Sprintf("%s-%d", userNamePrefix, i)
				if g.Username != expected {
					t.Errorf("expected displayName:%s got:%s", g.Username, expected)
				}
				if !g.CreatedAt.Equal(creationTime) {
					t.Errorf("expected created date:%s got:%s for %s", g.CreatedAt, creationTime, expected)
				}
			}
		})
	}
}

func TestAPIAuthService_ListGroupUsers(t *testing.T) {
	mockClient, s := NewTestApiService(t, false)
	const userNamePrefix = "userNamePrefix"
	const creationDate = 12345678
	amounts := []int{0, 1, 5}
	for _, amount := range amounts {
		t.Run(fmt.Sprintf("amount_%d", amount), func(t *testing.T) {
			users := make([]auth.User, amount)
			for i := 0; i < amount; i++ {
				users[i] = auth.User{
					CreationDate: creationDate,
					Username:     fmt.Sprintf("%s-%d", userNamePrefix, i),
				}
			}
			userList := auth.UserList{
				Pagination: auth.Pagination{},
				Results:    users,
			}
			response := &auth.ListGroupMembersResponse{
				HTTPResponse: &http.Response{StatusCode: http.StatusOK},
			}
			response.JSON200 = &userList
			const groupDisplayName = "groupFoo"
			mockClient.EXPECT().ListGroupMembersWithResponse(gomock.Any(), groupDisplayName, gomock.Any()).Return(response, nil)
			user, _, err := s.ListGroupUsers(context.Background(), groupDisplayName, &model.PaginationParams{})
			if err != nil {
				t.Fatalf("failed with error - %s", err)
			}
			creationTime := time.Unix(creationDate, 0)
			for i, g := range user {
				if g == nil {
					t.Fatalf("got nil user")
				}
				expected := fmt.Sprintf("%s-%d", userNamePrefix, i)
				if g.Username != expected {
					t.Errorf("expected displayName:%s got:%s", g.Username, expected)
				}
				if !g.CreatedAt.Equal(creationTime) {
					t.Errorf("expected created date:%s got:%s for %s", g.CreatedAt, creationTime, expected)
				}
			}
		})
	}
}

func TestAPIAuthService_AddUserToGroup(t *testing.T) {
	mockClient, s := NewTestApiService(t, false)
	mockErr := errors.New("this is a mock error")
	testTable := []struct {
		name        string
		groupName   string
		username    string
		mockErr     error
		statusCode  int
		expectedErr error
	}{
		{
			name:        "no_error",
			groupName:   "group_name",
			username:    "user_ame",
			mockErr:     nil,
			statusCode:  http.StatusCreated,
			expectedErr: nil,
		},
		{
			name:        "api_internal_error",
			groupName:   "gname",
			username:    "uname",
			mockErr:     mockErr,
			statusCode:  http.StatusInternalServerError,
			expectedErr: mockErr,
		},
		{
			name:        "not_found",
			groupName:   "no_group",
			username:    "username",
			mockErr:     nil,
			statusCode:  http.StatusNotFound,
			expectedErr: auth.ErrNotFound,
		},
	}
	for _, tt := range testTable {
		t.Run(tt.name, func(t *testing.T) {
			response := &auth.AddGroupMembershipResponse{
				Body: nil,
				HTTPResponse: &http.Response{
					StatusCode: tt.statusCode,
				},
			}
			mockClient.EXPECT().AddGroupMembershipWithResponse(gomock.Any(), tt.groupName, tt.username).Return(response, tt.mockErr)
			err := s.AddUserToGroup(context.Background(), tt.username, tt.groupName)
			if !errors.Is(err, tt.expectedErr) {
				t.Fatalf("returned different error as api got:%v, expected:%v", err, mockErr)
			}
		})
	}
}

func TestAPIAuthService_DeleteGroup(t *testing.T) {
	mockClient, s := NewTestApiService(t, false)
	mockErr := errors.New("this is a mock error")
	testTable := []struct {
		name        string
		groupName   string
		mockErr     error
		statusCode  int
		expectedErr error
	}{
		{
			name:        "no_error",
			groupName:   "group_name",
			mockErr:     nil,
			statusCode:  http.StatusNoContent,
			expectedErr: nil,
		},
		{
			name:        "api_error",
			groupName:   "group_name",
			mockErr:     mockErr,
			statusCode:  http.StatusInternalServerError,
			expectedErr: mockErr,
		},
		{
			name:        "not_found",
			groupName:   "no_group",
			mockErr:     nil,
			statusCode:  http.StatusNotFound,
			expectedErr: auth.ErrNotFound,
		},
	}
	for _, tt := range testTable {
		t.Run(tt.name, func(t *testing.T) {
			response := &auth.DeleteGroupResponse{
				Body: nil,
				HTTPResponse: &http.Response{
					StatusCode: tt.statusCode,
				},
			}
			mockClient.EXPECT().DeleteGroupWithResponse(gomock.Any(), tt.groupName).Return(response, tt.mockErr)
			err := s.DeleteGroup(context.Background(), tt.groupName)
			if !errors.Is(err, tt.expectedErr) {
				t.Fatalf("returned different error as api got:%v, expected:%v", err, mockErr)
			}
		})
	}
}

func TestAPIAuthService_RemoveUserFromGroup(t *testing.T) {
	mockClient, s := NewTestApiService(t, false)
	mockErr := errors.New("this is a mock error")
	testTable := []struct {
		name        string
		groupName   string
		username    string
		mockErr     error
		statusCode  int
		expectedErr error
	}{
		{
			name:        "no_error",
			groupName:   "group_name",
			username:    "user_name",
			mockErr:     nil,
			statusCode:  http.StatusNoContent,
			expectedErr: nil,
		},
		{
			name:        "api_error",
			groupName:   "group_name",
			username:    "user_name",
			mockErr:     mockErr,
			statusCode:  http.StatusInternalServerError,
			expectedErr: mockErr,
		},
		{
			name:        "not_found",
			groupName:   "group_name",
			username:    "user_name",
			mockErr:     nil,
			statusCode:  http.StatusNotFound,
			expectedErr: auth.ErrNotFound,
		},
	}
	for _, tt := range testTable {
		t.Run(tt.name, func(t *testing.T) {
			response := &auth.DeleteGroupMembershipResponse{
				Body: nil,
				HTTPResponse: &http.Response{
					StatusCode: tt.statusCode,
				},
			}
			mockClient.EXPECT().DeleteGroupMembershipWithResponse(gomock.Any(), tt.groupName, tt.username).Return(response, tt.mockErr)
			err := s.RemoveUserFromGroup(context.Background(), tt.username, tt.groupName)
			if !errors.Is(err, tt.expectedErr) {
				t.Fatalf("returned different error as api got:%v, expected:%v", err, mockErr)
			}
		})
	}
}

func TestAPIAuthService_ListUserGroups(t *testing.T) {
	mockClient, s := NewTestApiService(t, false)
	const groupNamePrefix = "groupNamePrefix"
	const creationDate = 12345678
	amounts := []int{0, 1, 5}
	for _, amount := range amounts {
		t.Run(fmt.Sprintf("amount_%d", amount), func(t *testing.T) {
			groups := make([]auth.Group, amount)
			for i := 0; i < amount; i++ {
				groups[i] = auth.Group{
					CreationDate: creationDate,
					Name:         fmt.Sprintf("%s-%d", groupNamePrefix, i),
				}
			}
			groupList := auth.GroupList{
				Pagination: auth.Pagination{},
				Results:    groups,
			}
			response := &auth.ListUserGroupsResponse{
				HTTPResponse: &http.Response{StatusCode: http.StatusOK},
			}
			response.JSON200 = &groupList
			const username = "userFoo"
			mockClient.EXPECT().ListUserGroupsWithResponse(gomock.Any(), username, gomock.Any()).Return(response, nil)
			group, _, err := s.ListUserGroups(context.Background(), username, &model.PaginationParams{})
			if err != nil {
				t.Fatalf("failed with error - %s", err)
			}

			creationTime := time.Unix(creationDate, 0)
			for i, g := range group {
				if g == nil {
					t.Fatalf("item %d - got nil group", i)
				}
				expected := fmt.Sprintf("%s-%d", groupNamePrefix, i)
				if g.DisplayName != expected {
					t.Errorf("expected displayName:%s got:%s", g.DisplayName, expected)
				}
				if !g.CreatedAt.Equal(creationTime) {
					t.Errorf("expected created date:%s got:%s for %s", g.CreatedAt, creationTime, expected)
				}
			}
		})
	}
}

func TestAPIAuthService_ListUserCredentials(t *testing.T) {
	mockClient, s := NewTestApiService(t, false)
	const accessKeyPrefix = "AKIA"
	const creationDate = 12345678
	amounts := []int{0, 1, 5}
	for _, amount := range amounts {
		t.Run(fmt.Sprintf("amount_%d", amount), func(t *testing.T) {
			credentials := make([]auth.Credentials, amount)
			for i := 0; i < amount; i++ {
				credentials[i] = auth.Credentials{
					CreationDate: creationDate,
					AccessKeyId:  fmt.Sprintf("%s-%d", accessKeyPrefix, i),
				}
			}
			credentialsList := auth.CredentialsList{
				Pagination: auth.Pagination{},
				Results:    credentials,
			}
			response := &auth.ListUserCredentialsResponse{
				HTTPResponse: &http.Response{StatusCode: http.StatusOK},
			}
			response.JSON200 = &credentialsList
			const username = "userFoo"
			mockClient.EXPECT().ListUserCredentialsWithResponse(gomock.Any(), username, gomock.Any()).Return(response, nil)
			resCredentials, _, err := s.ListUserCredentials(context.Background(), username, &model.PaginationParams{})
			if err != nil {
				t.Fatalf("failed with error - %s", err)
			}

			for i, g := range resCredentials {
				if g == nil {
					t.Fatalf("item %d - got nil credentials", i)
				}
				expected := fmt.Sprintf("%s-%d", accessKeyPrefix, i)
				if g.AccessKeyID != expected {
					t.Errorf("expected AccessKeyID:%s got:%s", expected, g.AccessKeyID)
				}
			}
		})
	}
}

func TestAPIAuthService_WritePolicy(t *testing.T) {
	mockClient, s := NewTestApiService(t, false)
	tests := []struct {
		name                   string
		policyName             string
		responseStatusCode     int
		firstStatementResource string
		firstStatementEffect   string
		firstStatementAction   []string
		responseName           string
		expectedErr            error
	}{
		{
			name:                   "successful",
			firstStatementAction:   []string{"action"},
			firstStatementEffect:   "effect",
			firstStatementResource: "resource",
			policyName:             "foo",
			responseName:           "foo",
			responseStatusCode:     http.StatusCreated,
			expectedErr:            nil,
		},
		{
			name:                   "invalid_policy",
			firstStatementAction:   []string{"action"},
			firstStatementEffect:   "effect",
			firstStatementResource: "resource",
			policyName:             "",
			responseStatusCode:     http.StatusBadRequest,
			expectedErr:            model.ErrValidationError, // TODO(Guys): change this once we change this to the right error
		},
		{
			name:                   "policy_exists",
			policyName:             "existingPolicy",
			firstStatementAction:   []string{"action"},
			firstStatementEffect:   "effect",
			firstStatementResource: "resource",
			responseStatusCode:     http.StatusConflict,
			expectedErr:            auth.ErrAlreadyExists,
		},
		{
			name:                   "internal_error",
			firstStatementAction:   []string{"action"},
			firstStatementEffect:   "effect",
			firstStatementResource: "resource",
			policyName:             "policy",
			responseStatusCode:     http.StatusInternalServerError,
			expectedErr:            auth.ErrUnexpectedStatusCode,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			creationTime := time.Unix(123456789, 0)
			response := &auth.CreatePolicyResponse{
				HTTPResponse: &http.Response{
					StatusCode: tt.responseStatusCode,
				},
				JSON201: &auth.Policy{
					Name: tt.responseName,
				},
			}
			mockClient.EXPECT().CreatePolicyWithResponse(gomock.Any(), gomock.Eq(auth.CreatePolicyJSONRequestBody{
				CreationDate: swag.Int64(creationTime.Unix()),
				Name:         tt.policyName,
				Statement: []auth.Statement{{
					Action:   tt.firstStatementAction,
					Effect:   tt.firstStatementEffect,
					Resource: tt.firstStatementResource,
				},
				},
			})).MaxTimes(1).Return(response, nil)
			ctx := context.Background()
			err := s.WritePolicy(ctx, &model.Policy{
				DisplayName: tt.policyName,
				CreatedAt:   creationTime,
				Statement: []model.Statement{{
					Action:   tt.firstStatementAction,
					Effect:   tt.firstStatementEffect,
					Resource: tt.firstStatementResource,
				}},
			})
			if !errors.Is(err, tt.expectedErr) {
				t.Fatalf("CreatePolicy: expected err: %v got: %v", tt.expectedErr, err)
			}
		})
	}
}

func TestAPIAuthService_GetPolicy(t *testing.T) {
	mockClient, s := NewTestApiService(t, false)
	tests := []struct {
		name                   string
		policyName             string
		firstStatementResource string
		firstStatementEffect   string
		firstStatementAction   []string
		responseStatusCode     int
		responseName           string
		expectedErr            error
	}{
		{
			name:                   "successful",
			policyName:             "foo",
			firstStatementAction:   []string{"action"},
			firstStatementEffect:   "effect",
			firstStatementResource: "resource",
			responseName:           "foo",
			responseStatusCode:     http.StatusOK,
			expectedErr:            nil,
		},
		{
			name:               "invalid_policy",
			policyName:         "",
			responseStatusCode: http.StatusBadRequest,
			expectedErr:        auth.ErrInvalidRequest,
		},
		{
			name:        "internal_error",
			policyName:  "policy",
			expectedErr: auth.ErrUnexpectedStatusCode,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			response := &auth.GetPolicyResponse{
				HTTPResponse: &http.Response{
					StatusCode: tt.responseStatusCode,
				},
				JSON200: &auth.Policy{
					Name: tt.responseName,
					Statement: []auth.Statement{
						{
							Action:   tt.firstStatementAction,
							Effect:   tt.firstStatementEffect,
							Resource: tt.firstStatementResource,
						},
					},
				},
			}
			mockClient.EXPECT().GetPolicyWithResponse(gomock.Any(), tt.policyName).Return(response, nil)
			ctx := context.Background()
			policy, err := s.GetPolicy(ctx, tt.policyName)
			if !errors.Is(err, tt.expectedErr) {
				t.Fatalf("GetPolicy: expected err: %v got: %v", tt.expectedErr, err)
			}
			if err != nil {
				return
			}

			if response.JSON200 == nil {
				t.Fatal("GetPolicy: unexpected return of nil policy")
			}
			policyEquals(t, *response.JSON200, policy)
		})
	}
}

var authPoliciesForTesting = []auth.Policy{{
	Statement: []auth.Statement{
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

func statementEquals(t *testing.T, authStatement []auth.Statement, modalStatement []model.Statement) {
	t.Helper()
	if len(authStatement) != len(modalStatement) {
		t.Errorf(" amoumt of statements:  (authPolicy)%d != (modelPolicy)%d", len(authStatement), len(modalStatement))
		return
	}
	for i, authS := range authStatement {
		if authS.Effect != modalStatement[i].Effect {
			t.Errorf("Effect  (authStatement)%s != (modelStatement)%s", modalStatement[i].Effect, authS.Effect)
		}
		if authS.Resource != modalStatement[i].Resource {
			t.Errorf("Resource  (authStatement)%s != (modelStatement)%s", modalStatement[i].Resource, authS.Resource)
		}
		if diff := deep.Equal(authS.Action, modalStatement[i].Action); diff != nil {
			t.Errorf("Action diff %s", diff)
		}
	}
}

func policyEquals(t *testing.T, authPolicy auth.Policy, modelPolicy *model.Policy) {
	t.Helper()
	if authPolicy.Name == "" && modelPolicy == nil {
		return
	}
	if modelPolicy == nil {
		t.Errorf("got nil modelPolicy nil comparing to authPolicy:%s", authPolicy.Name)
	}
	if authPolicy.Name != modelPolicy.DisplayName {
		t.Errorf("non equal name %s != %s", authPolicy.Name, modelPolicy.DisplayName)
	}
	statementEquals(t, authPolicy.Statement, modelPolicy.Statement)
}

func policyListsEquals(t *testing.T, authPolicies []auth.Policy, modelPolicies []*model.Policy) {
	t.Helper()
	if len(authPolicies) != len(modelPolicies) {
		t.Fatalf("got %d policies expected:%d", len(authPolicies), len(modelPolicies))
	}
	for i, ap := range authPolicies {
		policyEquals(t, ap, modelPolicies[i])
	}
}

func TestAPIAuthService_ListUserPolicies(t *testing.T) {
	mockClient, s := NewTestApiService(t, false)
	resPolicies := authPoliciesForTesting
	policyList := auth.PolicyList{
		Pagination: auth.Pagination{},
		Results:    resPolicies,
	}

	t.Run("policies", func(t *testing.T) {
		response := &auth.ListPoliciesResponse{
			HTTPResponse: &http.Response{StatusCode: http.StatusOK},
			JSON200:      &policyList,
		}
		mockClient.EXPECT().ListPoliciesWithResponse(gomock.Any(), gomock.Any()).Return(response, nil)
		policies, _, err := s.ListPolicies(context.Background(), &model.PaginationParams{})
		if err != nil {
			t.Fatalf("failed with error - %s", err)
		}
		policyListsEquals(t, resPolicies, policies)
	})

	t.Run("user policies", func(t *testing.T) {
		username := "username"
		response := &auth.ListUserPoliciesResponse{
			HTTPResponse: &http.Response{StatusCode: http.StatusOK},
			JSON200:      &policyList,
		}
		mockClient.EXPECT().ListUserPoliciesWithResponse(gomock.Any(), username, gomock.Any()).Return(response, nil)
		policies, _, err := s.ListUserPolicies(context.Background(), username, &model.PaginationParams{})
		if err != nil {
			t.Fatalf("failed with error - %s", err)
		}
		policyListsEquals(t, resPolicies, policies)
	})
	t.Run("group policies", func(t *testing.T) {
		response := &auth.ListGroupPoliciesResponse{
			HTTPResponse: &http.Response{StatusCode: http.StatusOK},
			JSON200:      &policyList,
		}
		const groupName = "groupName"
		mockClient.EXPECT().ListGroupPoliciesWithResponse(gomock.Any(), groupName, gomock.Any()).Return(response, nil)
		policies, _, err := s.ListGroupPolicies(context.Background(), groupName, &model.PaginationParams{})
		if err != nil {
			t.Fatalf("failed with error - %s", err)
		}
		policyListsEquals(t, resPolicies, policies)
	})
	t.Run("effective policies", func(t *testing.T) {
		response := &auth.ListUserPoliciesResponse{
			HTTPResponse: &http.Response{StatusCode: http.StatusOK},
			JSON200:      &policyList,
		}
		const username = "username"
		mockClient.EXPECT().ListUserPoliciesWithResponse(gomock.Any(), username, gomock.Any()).Return(response, nil)
		policies, _, err := s.ListEffectivePolicies(context.Background(), username, &model.PaginationParams{})
		if err != nil {
			t.Fatalf("failed with error - %s", err)
		}
		policyListsEquals(t, resPolicies, policies)
	})
	t.Run("all effective policies", func(t *testing.T) {
		response := &auth.ListUserPoliciesResponse{
			HTTPResponse: &http.Response{StatusCode: http.StatusOK},
			JSON200:      &policyList,
		}
		const username = "username"
		mockClient.EXPECT().ListUserPoliciesWithResponse(gomock.Any(), username, gomock.Any()).Return(response, nil)
		policies, _, err := s.ListEffectivePolicies(context.Background(), username, &model.PaginationParams{Amount: -1})
		if err != nil {
			t.Fatalf("failed with error - %s", err)
		}
		policyListsEquals(t, resPolicies, policies)
	})
}

func TestAPIAuthService_DeletePolicy(t *testing.T) {
	mockClient, s := NewTestApiService(t, false)
	mockErr := errors.New("this is a mock error")
	testTable := []struct {
		name        string
		policyName  string
		mockErr     error
		statusCode  int
		expectedErr error
	}{
		{
			name:        "no_error",
			policyName:  "policy_name",
			mockErr:     nil,
			statusCode:  http.StatusNoContent,
			expectedErr: nil,
		},
		{
			name:        "api_error",
			policyName:  "policy_name",
			mockErr:     mockErr,
			statusCode:  http.StatusInternalServerError,
			expectedErr: mockErr,
		},
		{
			name:        "not_found",
			policyName:  "no_policy",
			mockErr:     nil,
			statusCode:  http.StatusNotFound,
			expectedErr: auth.ErrNotFound,
		},
	}
	for _, tt := range testTable {
		t.Run(tt.name, func(t *testing.T) {
			response := &auth.DeletePolicyResponse{
				Body: nil,
				HTTPResponse: &http.Response{
					StatusCode: tt.statusCode,
				},
			}
			mockClient.EXPECT().DeletePolicyWithResponse(gomock.Any(), tt.policyName).Return(response, tt.mockErr)
			err := s.DeletePolicy(context.Background(), tt.policyName)
			if !errors.Is(err, tt.expectedErr) {
				t.Fatalf("returned different error as api got:%v, expected:%v", err, mockErr)
			}
		})
	}
}

func TestAPIAuthService_DetachPolicyFrom(t *testing.T) {
	mockClient, s := NewTestApiService(t, false)
	mockErr := errors.New("this is a mock error")
	testTable := []struct {
		name        string
		fromName    string
		username    string
		mockErr     error
		statusCode  int
		expectedErr error
	}{
		{
			name:        "no_error",
			fromName:    "from_name",
			username:    "username",
			mockErr:     nil,
			statusCode:  http.StatusNoContent,
			expectedErr: nil,
		},
		{
			name:        "api_error",
			fromName:    "from_name",
			username:    "username",
			mockErr:     mockErr,
			statusCode:  http.StatusInternalServerError,
			expectedErr: mockErr,
		},
		{
			name:        "not_found",
			fromName:    "no_from",
			username:    "userName",
			mockErr:     nil,
			statusCode:  http.StatusNotFound,
			expectedErr: auth.ErrNotFound,
		},
	}
	for _, tt := range testTable {
		t.Run("from user "+tt.name, func(t *testing.T) {
			response := &auth.DetachPolicyFromUserResponse{
				Body: nil,
				HTTPResponse: &http.Response{
					StatusCode: tt.statusCode,
				},
			}
			mockClient.EXPECT().DetachPolicyFromUserWithResponse(gomock.Any(), tt.username, tt.fromName).Return(response, tt.mockErr)
			err := s.DetachPolicyFromUser(context.Background(), tt.fromName, tt.username)
			if !errors.Is(err, tt.expectedErr) {
				t.Fatalf("returned different error as api got:%v, expected:%v", err, mockErr)
			}
		})
		t.Run("from group "+tt.name, func(t *testing.T) {
			response := &auth.DetachPolicyFromGroupResponse{
				Body: nil,
				HTTPResponse: &http.Response{
					StatusCode: tt.statusCode,
				},
			}
			mockClient.EXPECT().DetachPolicyFromGroupWithResponse(gomock.Any(), tt.username, tt.fromName).Return(response, tt.mockErr)
			err := s.DetachPolicyFromGroup(context.Background(), tt.fromName, tt.username)
			if !errors.Is(err, tt.expectedErr) {
				t.Fatalf("returned different error as api got:%v, expected:%v", err, mockErr)
			}
		})
	}
}

func TestAPIAuthService_AttachPolicyTo(t *testing.T) {
	mockClient, s := NewTestApiService(t, false)
	mockErr := errors.New("this is a mock error")
	testTable := []struct {
		name        string
		fromName    string
		username    string
		mockErr     error
		statusCode  int
		expectedErr error
	}{
		{
			name:        "no_error",
			fromName:    "from_name",
			username:    "username",
			mockErr:     nil,
			statusCode:  http.StatusCreated,
			expectedErr: nil,
		},
		{
			name:        "api_error",
			fromName:    "from_name",
			username:    "username",
			mockErr:     mockErr,
			statusCode:  http.StatusInternalServerError,
			expectedErr: mockErr,
		},
		{
			name:        "not_found",
			fromName:    "no_from",
			username:    "userName",
			mockErr:     nil,
			statusCode:  http.StatusNotFound,
			expectedErr: auth.ErrNotFound,
		},
	}
	for _, tt := range testTable {
		t.Run("to user "+tt.name, func(t *testing.T) {
			response := &auth.AttachPolicyToUserResponse{
				Body: nil,
				HTTPResponse: &http.Response{
					StatusCode: tt.statusCode,
				},
			}
			mockClient.EXPECT().AttachPolicyToUserWithResponse(gomock.Any(), tt.username, tt.fromName).Return(response, tt.mockErr)
			err := s.AttachPolicyToUser(context.Background(), tt.fromName, tt.username)
			if !errors.Is(err, tt.expectedErr) {
				t.Fatalf("returned different error as api got:%v, expected:%v", err, mockErr)
			}
		})
		t.Run("to group "+tt.name, func(t *testing.T) {
			response := &auth.AttachPolicyToGroupResponse{
				Body: nil,
				HTTPResponse: &http.Response{
					StatusCode: tt.statusCode,
				},
			}
			mockClient.EXPECT().AttachPolicyToGroupWithResponse(gomock.Any(), tt.username, tt.fromName).Return(response, tt.mockErr)
			err := s.AttachPolicyToGroup(context.Background(), tt.fromName, tt.username)
			if !errors.Is(err, tt.expectedErr) {
				t.Fatalf("returned different error as api got:%v, expected:%v", err, mockErr)
			}
		})
	}
}

func TestAPIAuthService_DeleteCredentials(t *testing.T) {
	mockClient, s := NewTestApiService(t, false)
	mockErr := errors.New("this is a mock error")
	testTable := []struct {
		name            string
		username        string
		credentialsName string
		mockErr         error
		statusCode      int
		expectedErr     error
	}{
		{
			name:            "no_error",
			username:        "username",
			credentialsName: "credentials_name",
			mockErr:         nil,
			statusCode:      http.StatusNoContent,
			expectedErr:     nil,
		},
		{
			name:            "invalid_user",
			username:        "",
			credentialsName: "credentials_name",
			mockErr:         mockErr,
			statusCode:      http.StatusInternalServerError,
			expectedErr:     mockErr,
		},
		{
			name:            "api_error",
			username:        "username",
			credentialsName: "credentials_name",
			mockErr:         mockErr,
			statusCode:      http.StatusInternalServerError,
			expectedErr:     mockErr,
		},
		{
			name:            "not_found",
			username:        "username",
			credentialsName: "no_credentials",
			mockErr:         nil,
			statusCode:      http.StatusNotFound,
			expectedErr:     auth.ErrNotFound,
		},
	}
	for _, tt := range testTable {
		t.Run(tt.name, func(t *testing.T) {
			response := &auth.DeleteCredentialsResponse{
				Body: nil,
				HTTPResponse: &http.Response{
					StatusCode: tt.statusCode,
				},
			}
			mockClient.EXPECT().DeleteCredentialsWithResponse(gomock.Any(), tt.username, tt.credentialsName).Return(response, tt.mockErr)
			err := s.DeleteCredentials(context.Background(), tt.username, tt.credentialsName)
			if !errors.Is(err, tt.expectedErr) {
				t.Fatalf("returned different error as api got:%v, expected:%v", err, mockErr)
			}
		})
	}
}

func TestAPIAuthService_CreateGroup(t *testing.T) {
	mockClient, s := NewTestApiService(t, false)
	tests := []struct {
		name               string
		groupName          string
		responseStatusCode int
		responseName       string
		expectedErr        error
	}{
		{
			name:               "successful",
			groupName:          "foo",
			responseName:       "foo",
			responseStatusCode: http.StatusCreated,
			expectedErr:        nil,
		},
		{
			name:               "invalid_group",
			groupName:          "",
			responseStatusCode: http.StatusBadRequest,
			expectedErr:        auth.ErrInvalidRequest,
		},
		{
			name:               "group_exists",
			groupName:          "existingGroup",
			responseStatusCode: http.StatusConflict,
			expectedErr:        auth.ErrAlreadyExists,
		},
		{
			name:               "internal_error",
			groupName:          "group",
			responseStatusCode: http.StatusInternalServerError,
			expectedErr:        auth.ErrUnexpectedStatusCode,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			response := &auth.CreateGroupResponse{
				HTTPResponse: &http.Response{
					StatusCode: tt.responseStatusCode,
				},
				JSON201: &auth.Group{
					Name: tt.responseName,
				},
			}
			mockClient.EXPECT().CreateGroupWithResponse(gomock.Any(), auth.CreateGroupJSONRequestBody{
				Id: tt.groupName,
			}).Return(response, nil)
			ctx := context.Background()
			err := s.CreateGroup(ctx, &model.Group{
				DisplayName: tt.groupName,
			})
			if !errors.Is(err, tt.expectedErr) {
				t.Fatalf("CreateGroup: expected err: %v got: %v", tt.expectedErr, err)
			}
		})
	}
}

func TestAPIAuthService_CreateCredentials(t *testing.T) {
	mockClient, s := NewTestApiService(t, false)
	tests := []struct {
		name               string
		username           string
		returnedAccessKey  string
		returnedSecretKey  string
		email              string
		friendlyName       string
		source             string
		responseStatusCode int
		responseName       string
		expectedResponseID string
		expectedErr        error
	}{
		{
			name:               "successful",
			username:           "foo",
			returnedAccessKey:  "AKIA",
			returnedSecretKey:  "AKIASECRET",
			email:              "foo@gmail.com",
			friendlyName:       "friendly foo",
			source:             "internal",
			responseName:       "foo",
			responseStatusCode: http.StatusCreated,
			expectedErr:        nil,
		},
		{
			name:               "invalid_username",
			username:           "",
			returnedAccessKey:  "AKIA",
			returnedSecretKey:  "AKIASECRET",
			email:              "foo@gmail.com",
			friendlyName:       "friendly foo",
			source:             "internal",
			responseStatusCode: http.StatusBadRequest,
			expectedErr:        auth.ErrInvalidRequest,
		},
		{
			name:               "internal_error",
			username:           "credentials",
			returnedAccessKey:  "AKIA",
			returnedSecretKey:  "AKIASECRET",
			email:              "foo@gmail.com",
			source:             "internal",
			responseStatusCode: http.StatusInternalServerError,
			expectedErr:        auth.ErrUnexpectedStatusCode,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			response := &auth.CreateCredentialsResponse{
				HTTPResponse: &http.Response{
					StatusCode: tt.responseStatusCode,
				},
				JSON201: &auth.CredentialsWithSecret{
					AccessKeyId:     tt.returnedAccessKey,
					SecretAccessKey: tt.returnedSecretKey,
				},
			}
			mockClient.EXPECT().CreateCredentialsWithResponse(gomock.Any(), tt.username, &auth.CreateCredentialsParams{}).Return(response, nil)
			ctx := context.Background()
			resCredentials, err := s.CreateCredentials(ctx, tt.username)
			if !errors.Is(err, tt.expectedErr) {
				t.Fatalf("CreateCredentials: expected err: %v got: %v", tt.expectedErr, err)
			}
			if err != nil {
				return
			}
			if resCredentials.AccessKeyID != tt.returnedAccessKey {
				t.Errorf("expected accessKeyID:%s, got:%s", tt.returnedAccessKey, resCredentials.AccessKeyID)
			}
			if resCredentials.SecretAccessKey != tt.returnedSecretKey {
				t.Errorf("expected secretKeyID:%s, got:%s", tt.returnedSecretKey, resCredentials.SecretAccessKey)
			}
		})
	}
}

func TestAPIAuthService_AddCredentials(t *testing.T) {
	mockClient, s := NewTestApiService(t, false)
	tests := []struct {
		name               string
		username           string
		returnedAccessKey  string
		returnedSecretKey  string
		accessKey          string
		secretKey          string
		email              string
		friendlyName       string
		source             string
		responseStatusCode int
		responseName       string
		expectedResponseID string
		expectedErr        error
	}{
		{
			name:               "successful",
			username:           "foo",
			returnedAccessKey:  "AKIA",
			returnedSecretKey:  "AKIASECRET",
			accessKey:          "AKIA",
			secretKey:          "AKIASECRET",
			email:              "foo@gmail.com",
			friendlyName:       "friendly foo",
			source:             "internal",
			responseName:       "foo",
			responseStatusCode: http.StatusCreated,
			expectedErr:        nil,
		},
		{
			name:               "invalid_username",
			username:           "",
			returnedAccessKey:  "AKIA",
			returnedSecretKey:  "AKIASECRET",
			accessKey:          "",
			secretKey:          "",
			email:              "foo@gmail.com",
			friendlyName:       "friendly foo",
			source:             "internal",
			responseStatusCode: http.StatusBadRequest,
			expectedErr:        auth.ErrInvalidRequest,
		},
		{
			name:               "credentials_exists",
			username:           "existingCredentials",
			returnedAccessKey:  "",
			returnedSecretKey:  "",
			accessKey:          "AKIA",
			secretKey:          "AKIASECRET",
			email:              "foo@gmail.com",
			friendlyName:       "friendly foo",
			source:             "internal",
			responseStatusCode: http.StatusConflict,
			expectedErr:        auth.ErrAlreadyExists,
		},
		{
			name:               "internal_error",
			username:           "credentials",
			returnedAccessKey:  "",
			returnedSecretKey:  "",
			accessKey:          "AKIA",
			secretKey:          "AKIASECRET",
			email:              "foo@gmail.com",
			source:             "internal",
			responseStatusCode: http.StatusInternalServerError,
			expectedErr:        auth.ErrUnexpectedStatusCode,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			response := &auth.CreateCredentialsResponse{
				HTTPResponse: &http.Response{
					StatusCode: tt.responseStatusCode,
				},
				JSON201: &auth.CredentialsWithSecret{
					AccessKeyId:     tt.returnedAccessKey,
					SecretAccessKey: tt.returnedSecretKey,
				},
			}
			mockClient.EXPECT().CreateCredentialsWithResponse(gomock.Any(), tt.username, &auth.CreateCredentialsParams{
				AccessKey: &tt.accessKey,
				SecretKey: &tt.secretKey,
			}).Return(response, nil)
			ctx := context.Background()
			resCredentials, err := s.AddCredentials(ctx, tt.username, tt.accessKey, tt.secretKey)
			if !errors.Is(err, tt.expectedErr) {
				t.Fatalf("CreateCredentials: expected err: %v got: %v", tt.expectedErr, err)
			}
			if err != nil {
				return
			}
			if resCredentials.AccessKeyID != tt.returnedAccessKey {
				t.Errorf("expected accessKeyID:%s, got:%s", tt.returnedAccessKey, resCredentials.AccessKeyID)
			}
			if resCredentials.SecretAccessKey != tt.returnedSecretKey {
				t.Errorf("expected secretKeyID:%s, got:%s", tt.returnedSecretKey, resCredentials.SecretAccessKey)
			}
		})
	}
}
