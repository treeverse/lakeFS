package auth_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"sort"
	"testing"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/davecgh/go-spew/spew"
	"github.com/go-test/deep"
	"github.com/google/uuid"
	"github.com/ory/dockertest/v3"
	authparams "github.com/treeverse/lakefs/pkg/auth/params"

	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/crypt"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/permissions"
	"github.com/treeverse/lakefs/pkg/testutil"
)

var (
	pool        *dockertest.Pool
	databaseURI string
	psql        = sq.StatementBuilder.PlaceholderFormat(sq.Dollar)
	someSecret  = []byte("some secret")

	userPoliciesForTesting = []*model.Policy{
		{
			Statement: model.Statements{
				{
					Action:   []string{"auth:DeleteUser"},
					Resource: "arn:lakefs:auth:::user/foobar",
					Effect:   model.StatementEffectAllow,
				},
			},
		},
		{
			Statement: model.Statements{
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

func setupService(t testing.TB, opts ...testutil.GetDBOption) auth.Service {
	adb, _ := testutil.GetDB(t, databaseURI, opts...)
	authService := auth.NewDBAuthService(adb, crypt.NewSecretStore(someSecret), authparams.ServiceCache{
		Enabled: false,
	})
	return authService
}

func userWithPolicies(t testing.TB, s auth.Service, policies []*model.Policy) string {
	ctx := context.Background()
	userName := uuid.New().String()
	err := s.CreateUser(ctx, &model.User{
		Username: userName,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, policy := range policies {
		p := &model.Policy{
			DisplayName: uuid.New().String(),
			Statement:   policy.Statement,
		}
		err := s.WritePolicy(ctx, p)
		if err != nil {
			t.Fatal(err)
		}
		err = s.AttachPolicyToUser(ctx, p.DisplayName, userName)
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

func TestDBAuthService_Authorize(t *testing.T) {
	ctx := context.Background()
	s := setupService(t)

	cases := []struct {
		name     string
		policies []*model.Policy
		request  func(userName string) *auth.AuthorizationRequest

		expectedAllowed bool
		expectedError   error
	}{
		{
			name: "basic_allowed",
			policies: []*model.Policy{
				{
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
					RequiredPermissions: []permissions.Permission{
						{
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
			policies: []*model.Policy{
				{
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
					RequiredPermissions: []permissions.Permission{
						{
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
			policies: []*model.Policy{
				{
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
					RequiredPermissions: []permissions.Permission{
						{
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
			policies: []*model.Policy{
				{
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
					RequiredPermissions: []permissions.Permission{
						{
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
			policies: []*model.Policy{
				{
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
					RequiredPermissions: []permissions.Permission{
						{
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
			policies: []*model.Policy{
				{
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
					RequiredPermissions: []permissions.Permission{
						{
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
			policies: []*model.Policy{
				{
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
					RequiredPermissions: []permissions.Permission{
						{
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
			policies: []*model.Policy{
				{
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
					RequiredPermissions: []permissions.Permission{
						{
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
			policies: []*model.Policy{
				{
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
					RequiredPermissions: []permissions.Permission{
						{
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
			policies: []*model.Policy{
				{
					Statement: model.Statements{
						{
							Action:   []string{"auth:DeleteUser"},
							Resource: "arn:lakefs:auth:::user/foobar",
							Effect:   model.StatementEffectAllow,
						},
					},
				},
				{
					Statement: model.Statements{
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
					RequiredPermissions: []permissions.Permission{
						{
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

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			uid := userWithPolicies(t, s, testCase.policies)
			request := testCase.request(uid)
			response, err := s.Authorize(ctx, request)
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
			s := setupService(t)
			for _, userName := range testCase.userNames {
				if err := s.CreateUser(ctx, &model.User{Username: userName}); err != nil {
					t.Fatalf("CreateUser(%s): %s", userName, err)
				}
			}
			gotList, _, err := s.ListUsers(ctx, &model.PaginationParams{Amount: -1})
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
		})
	}
}

func TestDBAuthService_ListUserCredentials(t *testing.T) {
	const userName = "accredited"
	s := setupService(t)
	ctx := context.Background()
	if err := s.CreateUser(ctx, &model.User{Username: userName}); err != nil {
		t.Fatalf("CreateUser(%s): %s", userName, err)
	}
	credential, err := s.CreateCredentials(ctx, userName)
	if err != nil {
		t.Errorf("CreateCredentials(%s): %s", userName, err)
	}
	credentials, _, err := s.ListUserCredentials(ctx, userName, &model.PaginationParams{Amount: -1})
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
	if credential.UserID != gotCredential.UserID {
		t.Errorf("expected to receive same user ID %d, got %d", credential.UserID, gotCredential.UserID)
	}
	// Issued dates are somewhat different, make sure not _too_ different.
	timeDiff := credential.IssuedDate.Sub(gotCredential.IssuedDate)
	if timeDiff > time.Second || timeDiff < -1*time.Second {
		t.Errorf("expected to receive issued date close to %s, got %s (diff %s)", credential.IssuedDate, gotCredential.IssuedDate, timeDiff)
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
			s := setupService(t)
			for _, groupName := range testCase.groupNames {
				if err := s.CreateGroup(ctx, &model.Group{DisplayName: groupName}); err != nil {
					t.Fatalf("CreateGroup(%s): %s", groupName, err)
				}
			}
			gotGroupNames := make([]string, 0, len(testCase.groupNames))
			groups, _, err := s.ListGroups(ctx, &model.PaginationParams{Amount: -1})
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
		})
	}
}

func TestDbAuthService_GetUser(t *testing.T) {
	s := setupService(t)
	ctx := context.Background()
	const userName = "foo"
	// Time should *not* have nanoseconds - otherwise we are comparing accuracy of golang
	// and Postgres time storage.
	ts := time.Date(2222, 2, 22, 22, 22, 22, 0, time.UTC)
	if err := s.CreateUser(ctx, &model.User{Username: userName, ID: -22, CreatedAt: ts}); err != nil {
		t.Fatalf("CreateUser(%s): %s", userName, err)
	}
	user, err := s.GetUser(ctx, userName)
	if err != nil {
		t.Fatalf("GetUser(%s): %s", userName, err)
	}
	if user.Username != userName {
		t.Errorf("GetUser(%s) returned user %+v with a different name", userName, user)
	}
	if user.CreatedAt.Sub(ts) != 0 {
		t.Errorf("expected user CreatedAt %s, got %+v", ts, user.CreatedAt)
	}
	if user.ID == -22 {
		t.Errorf("expected CreateUser ID:-22 to be dropped on server, got user %+v", user)
	}
}

func TestDbAuthService_GetUserById(t *testing.T) {
	s := setupService(t)
	ctx := context.Background()
	const userName = "foo"
	// Time should *not* have nanoseconds - otherwise we are comparing accuracy of golang
	// and Postgres time storage.
	ts := time.Date(2222, 2, 22, 22, 22, 22, 0, time.UTC)
	if err := s.CreateUser(ctx, &model.User{Username: userName, ID: -22, CreatedAt: ts}); err != nil {
		t.Fatalf("CreateUser(%s): %s", userName, err)
	}
	user, err := s.GetUser(ctx, userName)
	if err != nil {
		t.Fatalf("GetUser(%s): %s", userName, err)
	}
	gotUser, err := s.GetUserByID(ctx, user.ID)
	if err != nil {
		t.Errorf("GetUserById(%d): %s", user.ID, err)
	}
	if diffs := deep.Equal(user, gotUser); diffs != nil {
		t.Errorf("got different user by name and by ID: %s", diffs)
	}
}

func TestDBAuthService_DeleteUser(t *testing.T) {
	s := setupService(t)
	const userName = "foo"
	ctx := context.Background()
	if err := s.CreateUser(ctx, &model.User{Username: userName}); err != nil {
		t.Fatalf("CreateUser(%s): %s", userName, err)
	}
	_, err := s.GetUser(ctx, userName)
	if err != nil {
		t.Fatalf("GetUser(%s) before deletion: %s", userName, err)
	}
	if err = s.DeleteUser(ctx, userName); err != nil {
		t.Errorf("DeleteUser(%s): %s", userName, err)
	}
	_, err = s.GetUser(ctx, userName)
	if err == nil {
		t.Errorf("GetUser(%s) succeeded after DeleteUser", userName)
	} else if !errors.Is(err, auth.ErrNotFound) {
		t.Errorf("GetUser(%s) after deletion: %s", userName, err)
	}
}

func BenchmarkDBAuthService_ListEffectivePolicies(b *testing.B) {
	// setup user with policies for benchmark
	adb, _ := testutil.GetDB(b, databaseURI)
	serviceWithoutCache := auth.NewDBAuthService(adb, crypt.NewSecretStore(someSecret), authparams.ServiceCache{
		Enabled: false,
	})
	serviceWithCache := auth.NewDBAuthService(adb, crypt.NewSecretStore(someSecret), authparams.ServiceCache{
		Enabled:        true,
		Size:           1024,
		TTL:            20 * time.Second,
		EvictionJitter: 3 * time.Second,
	})
	serviceWithCacheLowTTL := auth.NewDBAuthService(adb, crypt.NewSecretStore(someSecret), authparams.ServiceCache{
		Enabled:        true,
		Size:           1024,
		TTL:            1 * time.Millisecond,
		EvictionJitter: 1 * time.Millisecond,
	})
	userName := userWithPolicies(b, serviceWithoutCache, userPoliciesForTesting)

	b.Run("without_cache", func(b *testing.B) {
		benchmarkListEffectivePolicies(b, serviceWithoutCache, userName)
	})
	b.Run("with_cache", func(b *testing.B) {
		benchmarkListEffectivePolicies(b, serviceWithCache, userName)
	})
	b.Run("without_cache_low_ttl", func(b *testing.B) {
		benchmarkListEffectivePolicies(b, serviceWithCacheLowTTL, userName)
	})
}

func benchmarkListEffectivePolicies(b *testing.B, s *auth.DBAuthService, userName string) {
	b.ResetTimer()
	ctx := context.Background()
	for n := 0; n < b.N; n++ {
		_, _, err := s.ListEffectivePolicies(ctx, userName, &model.PaginationParams{Amount: -1})
		if err != nil {
			b.Fatal("Failed to list effective policies", err)
		}
	}
}
