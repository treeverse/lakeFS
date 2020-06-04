package auth_test

import (
	"fmt"
	"os"
	"testing"

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
	roleName := uuid.New().String()
	err := s.CreateRole(&model.Role{
		DisplayName: roleName,
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
		err = s.AttachPolicyToRole(roleName, p.DisplayName)
		if err != nil {
			t.Fatal(err)
		}
	}
	userName := uuid.New().String()
	err = s.CreateUser(&model.User{
		DisplayName: userName,
	})
	if err != nil {
		t.Fatal(err)
	}
	err = s.AttachRoleToUser(roleName, userName)
	if err != nil {
		t.Fatal(err)
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
			name: "basic_allowed_role",
			policies: []*model.Policy{
				{
					Action:   []string{"repos:Write"},
					Resource: "arn:lakefs:repos:::myrepo",
					Effect:   true,
				},
			},
			request: func(userName string) *auth.AuthorizationRequest {
				return &auth.AuthorizationRequest{
					UserDisplayName: userName,
					Action:          "repos:Write",
					Resource:        "arn:lakefs:repos:::myrepo",
				}
			},
			expectedAllowed: true,
			expectedError:   nil,
		},
		{
			name: "basic_disallowed_role",
			policies: []*model.Policy{
				{
					Action:   []string{"repos:Write"},
					Resource: "arn:lakefs:repos:::myrepo",
					Effect:   false,
				},
				{
					Action:   []string{"repos:Write"},
					Resource: "arn:lakefs:repos:::myrepo",
					Effect:   true,
				},
			},
			request: func(userName string) *auth.AuthorizationRequest {
				return &auth.AuthorizationRequest{
					UserDisplayName: userName,
					Action:          "repos:Write",
					Resource:        "arn:lakefs:repos:::myrepo",
				}
			},
			expectedAllowed: false,
			expectedError:   auth.ErrInsufficientPermissions,
		},
		{
			name: "policy_with_wildcard",
			policies: []*model.Policy{
				{
					Action:   []string{"repos:Write"},
					Resource: "arn:lakefs:repos:::*",
					Effect:   true,
				},
			},
			request: func(userName string) *auth.AuthorizationRequest {
				return &auth.AuthorizationRequest{
					UserDisplayName: userName,
					Action:          "repos:Write",
					Resource:        "arn:lakefs:repos:::myrepo",
				}
			},
			expectedAllowed: true,
			expectedError:   nil,
		},
		{
			name: "policy_with_invalid_user",
			policies: []*model.Policy{
				{
					Action:   []string{"repos:Write"},
					Resource: "arn:lakefs:repos:::${user}",
					Effect:   true,
				},
			},
			request: func(userName string) *auth.AuthorizationRequest {
				return &auth.AuthorizationRequest{
					Action:   "repos:Write",
					Resource: "arn:lakefs:repos:::myrepo",
				}
			},
			expectedAllowed: false,
			expectedError:   auth.ErrInsufficientPermissions,
		},
		{
			name: "policy_with_valid_user",
			policies: []*model.Policy{
				{
					Action:   []string{"repos:Write"},
					Resource: "arn:lakefs:repos:::${user}",
					Effect:   true,
				},
			},
			request: func(userName string) *auth.AuthorizationRequest {
				return &auth.AuthorizationRequest{
					UserDisplayName: userName,
					Action:          "repos:Write",
					Resource:        fmt.Sprintf("arn:lakefs:repos:::%s", userName),
				}
			},
			expectedAllowed: true,
			expectedError:   nil,
		},
		{
			name: "policy_with_other_user",
			policies: []*model.Policy{
				{
					Action:   []string{"repos:Write"},
					Resource: "arn:lakefs:repos:::${user}",
					Effect:   true,
				},
			},
			request: func(userName string) *auth.AuthorizationRequest {
				return &auth.AuthorizationRequest{
					UserDisplayName: userName,
					Action:          "repos:Write",
					Resource:        fmt.Sprintf("arn:lakefs:repos:::%sxxxx", userName),
				}
			},
			expectedAllowed: false,
			expectedError:   auth.ErrInsufficientPermissions,
		},
		{
			name: "policy_with_wildcard",
			policies: []*model.Policy{
				{
					Action:   []string{"repos:Write"},
					Resource: "arn:lakefs:repos:::user/*",
					Effect:   true,
				},
			},
			request: func(userName string) *auth.AuthorizationRequest {
				return &auth.AuthorizationRequest{
					UserDisplayName: userName,
					Action:          "repos:Write",
					Resource:        "arn:lakefs:repos:::user/someUser",
				}
			},
			expectedAllowed: true,
			expectedError:   nil,
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
