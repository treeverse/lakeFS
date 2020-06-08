package auth_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/treeverse/lakefs/permissions"

	"github.com/google/uuid"

	"github.com/ory/dockertest/v3"
	"github.com/treeverse/lakefs/auth"
	"github.com/treeverse/lakefs/auth/crypt"
	"github.com/treeverse/lakefs/auth/model"
	"github.com/treeverse/lakefs/config"
	"github.com/treeverse/lakefs/logging"
	"github.com/treeverse/lakefs/testutil"
)

var (
	pool        *dockertest.Pool
	databaseUri string
)

func TestMain(m *testing.M) {
	var err error
	var closer func()
	pool, err = dockertest.NewPool("")
	if err != nil {
		logging.Default().Fatalf("Could not connect to Docker: %s", err)
	}
	databaseUri, closer = testutil.GetDBInstance(pool)
	code := m.Run()
	closer() // cleanup
	os.Exit(code)
}

func setupService(t *testing.T, opts ...testutil.GetDBOption) auth.Service {
	adb, _ := testutil.GetDB(t, databaseUri, config.SchemaAuth, opts...)
	authService := auth.NewDBAuthService(adb, crypt.NewSecretStore([]byte("some secret")))
	return authService
}

func userWithPolicies(t *testing.T, s auth.Service, policies []*model.Policy) string {
	userName := uuid.New().String()
	err := s.CreateUser(&model.User{
		DisplayName: userName,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, policy := range policies {
		p := &model.Policy{
			DisplayName: uuid.New().String(),
			Action:      policy.Action,
			Resource:    policy.Resource,
			Effect:      policy.Effect,
		}
		err := s.CreatePolicy(p)
		if err != nil {
			t.Fatal(err)
		}
		err = s.AttachPolicyToUser(p.DisplayName, userName)
		if err != nil {
			t.Fatal(err)
		}
	}

	return userName
}

func TestDBAuthService_Authorize(t *testing.T) {
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
					Action:   []string{"fs:WriteObject"},
					Resource: "arn:lakefs:fs:::repository/foo/object/bar",
					Effect:   true,
				},
			},
			request: func(userName string) *auth.AuthorizationRequest {
				return &auth.AuthorizationRequest{
					UserDisplayName: userName,
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
					Action:   []string{"fs:WriteObject"},
					Resource: "arn:lakefs:fs:::repository/foo/object/bar",
					Effect:   false,
				},
			},
			request: func(userName string) *auth.AuthorizationRequest {
				return &auth.AuthorizationRequest{
					UserDisplayName: userName,
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
					Action:   []string{"fs:WriteObject"},
					Resource: "arn:lakefs:fs:::repository/foo/object/*",
					Effect:   true,
				},
			},
			request: func(userName string) *auth.AuthorizationRequest {
				return &auth.AuthorizationRequest{
					UserDisplayName: userName,
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
					Action:   []string{"auth:CreateUser"},
					Resource: "arn:lakefs:auth:::user/${user}",
					Effect:   true,
				},
			},
			request: func(userName string) *auth.AuthorizationRequest {
				return &auth.AuthorizationRequest{
					UserDisplayName: userName,
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
					Action:   []string{"auth:CreateUser"},
					Resource: "arn:lakefs:auth:::user/${user}",
					Effect:   true,
				},
			},
			request: func(userName string) *auth.AuthorizationRequest {
				return &auth.AuthorizationRequest{
					UserDisplayName: userName,
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
					Action:   []string{"auth:CreateUser"},
					Resource: "arn:lakefs:auth:::user/${user}",
					Effect:   true,
				},
			},
			request: func(userName string) *auth.AuthorizationRequest {
				return &auth.AuthorizationRequest{
					UserDisplayName: userName,
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
					Action:   []string{"auth:CreateUser"},
					Resource: "arn:lakefs:auth:::user/*",
					Effect:   true,
				},
			},
			request: func(userName string) *auth.AuthorizationRequest {
				return &auth.AuthorizationRequest{
					UserDisplayName: userName,
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
					Action:   []string{"auth:Create*"},
					Resource: "arn:lakefs:auth:::user/foobar",
					Effect:   true,
				},
			},
			request: func(userName string) *auth.AuthorizationRequest {
				return &auth.AuthorizationRequest{
					UserDisplayName: userName,
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
					Action:   []string{"auth:Create*"},
					Resource: "arn:lakefs:auth:::user/foobar",
					Effect:   true,
				},
			},
			request: func(userName string) *auth.AuthorizationRequest {
				return &auth.AuthorizationRequest{
					UserDisplayName: userName,
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
					Action:   []string{"auth:DeleteUser"},
					Resource: "arn:lakefs:auth:::user/foobar",
					Effect:   true,
				},
				{
					Action:   []string{"auth:*"},
					Resource: "*",
					Effect:   false,
				},
			},
			request: func(userName string) *auth.AuthorizationRequest {
				return &auth.AuthorizationRequest{
					UserDisplayName: userName,
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
			response, err := s.Authorize(request)
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
