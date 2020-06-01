package testutil

import (
	"github.com/treeverse/lakefs/auth"
	authmodel "github.com/treeverse/lakefs/auth/model"
	"github.com/treeverse/lakefs/permissions"
	"testing"
	"time"
)

func CreateDefaultAdminUser(authService auth.Service, t *testing.T) *authmodel.Credential {
	// create user
	user := &authmodel.User{
		CreatedAt:   time.Now(),
		DisplayName: "admin",
	}
	Must(t, authService.CreateUser(user))

	// create role
	role := &authmodel.Role{
		DisplayName: "Admins",
	}
	Must(t, authService.CreateRole(role))

	// attach policies
	policy := &authmodel.Policy{
		CreatedAt:   time.Now(),
		DisplayName: "AdminFullAccess",
		Action: []string{
			string(permissions.ManageRepos),
			string(permissions.ReadRepo),
			string(permissions.WriteRepo),
		},
		Resource: "arn:lakefs:repos:::*",
		Effect:   true,
	}

	Must(t, authService.CreatePolicy(policy))
	Must(t, authService.AttachPolicyToRole(role.DisplayName, policy.DisplayName))

	// assign user to role
	Must(t, authService.AttachRoleToUser(role.DisplayName, user.DisplayName))

	creds, err := authService.CreateCredentials(user.DisplayName)
	if err != nil {
		t.Fatal(err)
	}
	return creds
}
