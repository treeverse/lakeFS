package setup

import (
	"context"
	"fmt"
	"time"

	authacl "github.com/treeverse/lakefs/contrib/auth/acl"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/permissions"
)

const (
	AdminsGroup     = "Admins"
	SuperUsersGroup = "SuperUsers"
	DevelopersGroup = "Developers"
	ViewersGroup    = "Viewers"
)

func createGroups(ctx context.Context, authService auth.Service, groups []*model.Group) error {
	for _, group := range groups {
		_, err := authService.CreateGroup(ctx, group)
		if err != nil {
			return err
		}
	}
	return nil
}

func createPolicies(ctx context.Context, authService auth.Service, policies []*model.Policy) error {
	for _, policy := range policies {
		err := authService.WritePolicy(ctx, policy, false)
		if err != nil {
			return err
		}
	}
	return nil
}

func CreateRBACBasePolicies(ctx context.Context, authService auth.Service, ts time.Time) error {
	all := []string{permissions.All}

	return createPolicies(ctx, authService, []*model.Policy{
		{
			CreatedAt:   ts,
			DisplayName: "FSFullAccess",
			Statement:   auth.MakeStatementForPolicyTypeOrDie("FSFullAccess", all),
		},
		{
			CreatedAt:   ts,
			DisplayName: "FSReadWriteAll",
			Statement:   auth.MakeStatementForPolicyTypeOrDie("FSReadWrite", all),
		},
		{
			CreatedAt:   ts,
			DisplayName: "FSReadAll",
			Statement:   auth.MakeStatementForPolicyTypeOrDie("FSRead", all),
		},
		{
			CreatedAt:   ts,
			DisplayName: "RepoManagementFullAccess",
			Statement: model.Statements{
				{
					Action: []string{
						"ci:*",
						"retention:*",
						"branches:*",
						"fs:ReadConfig",
					},
					Resource: permissions.All,
					Effect:   model.StatementEffectAllow,
				},
			},
		},
		{
			CreatedAt:   ts,
			DisplayName: "RepoManagementReadAll",
			Statement:   auth.MakeStatementForPolicyTypeOrDie("RepoManagementRead", all),
		},
		{
			CreatedAt:   ts,
			DisplayName: "AuthFullAccess",
			Statement: model.Statements{
				{
					Action: []string{
						"auth:*",
					},
					Resource: permissions.All,
					Effect:   model.StatementEffectAllow,
				},
			},
		},
		{
			CreatedAt:   ts,
			DisplayName: "AuthManageOwnCredentials",
			Statement: auth.MakeStatementForPolicyTypeOrDie(
				"AuthManageOwnCredentials",
				[]string{permissions.UserArn("${user}")},
			),
		},
	})
}

func attachPolicies(ctx context.Context, authService auth.Service, groupID string, policyIDs []string) error {
	for _, policyID := range policyIDs {
		err := authService.AttachPolicyToGroup(ctx, policyID, groupID)
		if err != nil {
			return err
		}
	}
	return nil
}

func CreateRBACBaseGroups(ctx context.Context, authService auth.Service, ts time.Time) error {
	err := createGroups(ctx, authService, []*model.Group{
		{CreatedAt: ts, DisplayName: AdminsGroup},
		{CreatedAt: ts, DisplayName: SuperUsersGroup},
		{CreatedAt: ts, DisplayName: DevelopersGroup},
		{CreatedAt: ts, DisplayName: ViewersGroup},
	})
	if err != nil {
		return err
	}

	err = CreateRBACBasePolicies(ctx, authService, ts)
	if err != nil {
		return err
	}

	err = attachPolicies(ctx, authService, "Admins", []string{"FSFullAccess", "AuthFullAccess", "RepoManagementFullAccess"})
	if err != nil {
		return err
	}
	err = attachPolicies(ctx, authService, "SuperUsers", []string{"FSFullAccess", "AuthManageOwnCredentials", "RepoManagementReadAll"})
	if err != nil {
		return err
	}
	err = attachPolicies(ctx, authService, "Developers", []string{"FSReadWriteAll", "AuthManageOwnCredentials", "RepoManagementReadAll"})
	if err != nil {
		return err
	}
	err = attachPolicies(ctx, authService, "Viewers", []string{"FSReadAll", "AuthManageOwnCredentials"})
	if err != nil {
		return err
	}

	return nil
}

// CreateAdminUser setup base groups, policies and create admin user
func CreateAdminUser(ctx context.Context, authService auth.Service, cfg *config.Config, superuser *model.SuperuserConfiguration) (*model.Credential, error) {
	// Set up the basic groups and policies
	now := time.Now()
	err := CreateBaseGroups(ctx, authService, cfg, now)
	if err != nil {
		return nil, err
	}

	return AddAdminUser(ctx, authService, superuser, true)
}

func AddAdminUser(ctx context.Context, authService auth.Service, user *model.SuperuserConfiguration, addToAdmins bool) (*model.Credential, error) {
	// create admin user
	user.Source = "internal"
	_, err := authService.CreateUser(ctx, &user.User)
	if err != nil {
		return nil, fmt.Errorf("create user - %w", err)
	}

	if addToAdmins {
		// verify the admin group exists
		_, err = authService.GetGroup(ctx, AdminsGroup)
		if err != nil {
			return nil, fmt.Errorf("admin group - %w", err)
		}

		err = authService.AddUserToGroup(ctx, user.Username, AdminsGroup)
		if err != nil {
			return nil, fmt.Errorf("add user to group - %w", err)
		}
	}

	var creds *model.Credential
	if user.AccessKeyID == "" {
		// Generate and return a key pair
		creds, err = authService.CreateCredentials(ctx, user.Username)
		if err != nil {
			return nil, fmt.Errorf("create credentials for %s: %w", user.Username, err)
		}
	} else {
		creds, err = authService.AddCredentials(ctx, user.Username, user.AccessKeyID, user.SecretAccessKey)
		if err != nil {
			return nil, fmt.Errorf("add credentials for %s: %w", user.Username, err)
		}
	}
	return creds, nil
}

func CreateInitialAdminUser(ctx context.Context, authService auth.Service, cfg *config.Config, metadataManger auth.MetadataManager, username string) (*model.Credential, error) {
	return CreateInitialAdminUserWithKeys(ctx, authService, cfg, metadataManger, username, nil, nil)
}

func CreateInitialAdminUserWithKeys(ctx context.Context, authService auth.Service, cfg *config.Config, metadataManger auth.MetadataManager, username string, accessKeyID *string, secretAccessKey *string) (*model.Credential, error) {
	adminUser := &model.SuperuserConfiguration{
		User: model.User{
			CreatedAt: time.Now(),
			Username:  username,
		},
	}
	if accessKeyID != nil && secretAccessKey != nil {
		adminUser.AccessKeyID = *accessKeyID
		adminUser.SecretAccessKey = *secretAccessKey
	}

	// create first admin user
	var (
		cred *model.Credential
		err  error
	)
	// TODO (niro): Handle with creds (what do we want to do with custom creds?)
	if cfg.IsAuthBasic() {
		if cred, err = AddAdminUser(ctx, authService, adminUser, false); err != nil {
			return nil, err
		}
	} else if cred, err = CreateAdminUser(ctx, authService, cfg, adminUser); err != nil {
		return nil, err
	}

	// update setup timestamp
	if err = metadataManger.UpdateSetupTimestamp(ctx, time.Now()); err != nil {
		logging.FromContext(ctx).WithError(err).Error("Failed the update setup timestamp")
	}
	return cred, err
}

func CreateBaseGroups(ctx context.Context, authService auth.Service, cfg *config.Config, ts time.Time) error {
	// TODO (niro): need to remove this when transitioning to external auth ACLs server
	if cfg.IsAuthUISimplified() && !cfg.IsAuthBasic() && cfg.Auth.API.Endpoint == "" {
		return authacl.CreateACLBaseGroups(ctx, authService, ts)
	}
	if !cfg.IsAdvancedAuth() {
		return nil
	}
	return CreateRBACBaseGroups(ctx, authService, ts)
}
