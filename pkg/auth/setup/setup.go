package setup

import (
	"context"
	"fmt"
	"time"

	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/acl"
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
		err := authService.CreateGroup(ctx, group)
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
						"fs:ReadConfigAction",
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

func SetupRBACBaseGroups(ctx context.Context, authService auth.Service, ts time.Time) error {
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

func SetupACLBaseGroups(ctx context.Context, authService auth.Service, ts time.Time) error {
	if err := authService.CreateGroup(ctx, &model.Group{CreatedAt: ts, DisplayName: acl.ACLAdminsGroup}); err != nil {
		return fmt.Errorf("setup: create base ACL group %s: %w", acl.ACLAdminsGroup, err)
	}
	if err := acl.WriteGroupACL(ctx, authService, acl.ACLAdminsGroup, model.ACL{Permission: acl.ACLAdmin}, ts, false); err != nil {
		return fmt.Errorf("setup: %w", err)
	}

	if err := authService.CreateGroup(ctx, &model.Group{CreatedAt: ts, DisplayName: acl.ACLSupersGroup}); err != nil {
		return fmt.Errorf("setup: create base ACL group %s: %w", acl.ACLSupersGroup, err)
	}
	if err := acl.WriteGroupACL(ctx, authService, acl.ACLSupersGroup, model.ACL{Permission: acl.ACLSuper}, ts, false); err != nil {
		return fmt.Errorf("setup: %w", err)
	}

	if err := authService.CreateGroup(ctx, &model.Group{CreatedAt: ts, DisplayName: acl.ACLWritersGroup}); err != nil {
		return fmt.Errorf("setup: create base ACL group %s: %w", acl.ACLWritersGroup, err)
	}
	if err := acl.WriteGroupACL(ctx, authService, acl.ACLWritersGroup, model.ACL{Permission: acl.ACLWrite}, ts, false); err != nil {
		return fmt.Errorf("setup: %w", err)
	}

	if err := authService.CreateGroup(ctx, &model.Group{CreatedAt: ts, DisplayName: acl.ACLReadersGroup}); err != nil {
		return fmt.Errorf("create base ACL group %s: %w", acl.ACLReadersGroup, err)
	}
	if err := acl.WriteGroupACL(ctx, authService, acl.ACLReadersGroup, model.ACL{Permission: acl.ACLRead}, ts, false); err != nil {
		return fmt.Errorf("setup: %w", err)
	}

	return nil
}

func SetupAdminUser(ctx context.Context, authService auth.Service, cfg *config.Config, superuser *model.SuperuserConfiguration) (*model.Credential, error) {
	now := time.Now()

	// Set up the basic groups and policies
	err := SetupBaseGroups(ctx, authService, cfg, now)
	if err != nil {
		return nil, err
	}

	return AddAdminUser(ctx, authService, superuser)
}

func AddAdminUser(ctx context.Context, authService auth.Service, user *model.SuperuserConfiguration) (*model.Credential, error) {
	const adminGroupName = "Admins"

	// verify admin group exists
	_, err := authService.GetGroup(ctx, adminGroupName)
	if err != nil {
		return nil, fmt.Errorf("admin group - %w", err)
	}

	// create admin user
	user.Source = "internal"
	_, err = authService.CreateUser(ctx, &user.User)
	if err != nil {
		return nil, fmt.Errorf("create user - %w", err)
	}
	err = authService.AddUserToGroup(ctx, user.Username, adminGroupName)
	if err != nil {
		return nil, fmt.Errorf("add user to group - %w", err)
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
	adminUser := &model.SuperuserConfiguration{User: model.User{
		CreatedAt: time.Now(),
		Username:  username,
	}}
	if accessKeyID != nil && secretAccessKey != nil {
		adminUser.AccessKeyID = *accessKeyID
		adminUser.SecretAccessKey = *secretAccessKey
	}
	// create first admin user
	cred, err := SetupAdminUser(ctx, authService, cfg, adminUser)
	if err != nil {
		return nil, err
	}

	// update setup timestamp
	if err := metadataManger.UpdateSetupTimestamp(ctx, time.Now()); err != nil {
		logging.Default().WithError(err).Error("Failed the update setup timestamp")
	}
	return cred, err
}

func SetupBaseGroups(ctx context.Context, authService auth.Service, cfg *config.Config, ts time.Time) error {
	if cfg.IsAuthUISimplified() {
		return SetupACLBaseGroups(ctx, authService, ts)
	}
	return SetupRBACBaseGroups(ctx, authService, ts)
}
