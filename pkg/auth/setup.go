package auth

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/permissions"
)

const (
	AdminsGroup     = "Admins"
	SuperUsersGroup = "SuperUsers"
	DevelopersGroup = "Developers"
	ViewersGroup    = "Viewers"
)

var (
	ErrStatementNotFound = errors.New("statement not found")
)

func createGroups(ctx context.Context, authService Service, groups []*model.Group) error {
	for _, group := range groups {
		err := authService.CreateGroup(ctx, group)
		if err != nil {
			return err
		}
	}
	return nil
}

func createPolicies(ctx context.Context, authService Service, policies []*model.Policy) error {
	for _, policy := range policies {
		err := authService.WritePolicy(ctx, policy, false)
		if err != nil {
			return err
		}
	}
	return nil
}

func attachPolicies(ctx context.Context, authService Service, groupID string, policyIDs []string) error {
	for _, policyID := range policyIDs {
		err := authService.AttachPolicyToGroup(ctx, policyID, groupID)
		if err != nil {
			return err
		}
	}
	return nil
}

// statementForPolicyType holds the Statement for a policy by its name,
// without the required ARN.
var statementByName = map[string]model.Statement{
	"AllAccess": {
		Action: []string{"fs:*", "auth:*", "ci:*", "retention:*", "branches:*"},
		Effect: model.StatementEffectAllow,
	},
	"FSFullAccess": {
		Action: []string{
			"fs:*",
		},
		Effect: model.StatementEffectAllow,
	},
	"FSReadWrite": {
		Action: []string{
			permissions.ListRepositoriesAction,
			permissions.ReadRepositoryAction,
			permissions.ReadCommitAction,
			permissions.ListBranchesAction,
			permissions.ListTagsAction,
			permissions.ListObjectsAction,
			permissions.ReadObjectAction,
			permissions.WriteObjectAction,
			permissions.DeleteObjectAction,
			permissions.RevertBranchAction,
			permissions.ReadBranchAction,
			permissions.ReadTagAction,
			permissions.CreateBranchAction,
			permissions.CreateTagAction,
			permissions.DeleteBranchAction,
			permissions.DeleteTagAction,
			permissions.CreateCommitAction,
		},
		Effect: model.StatementEffectAllow,
	},
	"FSRead": {
		Action: []string{
			"fs:List*",
			"fs:Read*",
		},

		Effect: model.StatementEffectAllow,
	},
	"AuthManageOwnCredentials": {
		Action: []string{
			permissions.CreateCredentialsAction,
			permissions.DeleteCredentialsAction,
			permissions.ListCredentialsAction,
			permissions.ReadCredentialsAction,
		},
		Effect: model.StatementEffectAllow,
	},
}

// MakeStatementForPolicyType returns statements for policy type typ,
// limited to resources.
func MakeStatementForPolicyType(typ string, resources []string) (model.Statements, error) {
	statement, ok := statementByName[typ]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrStatementNotFound, typ)
	}
	statements := make(model.Statements, len(resources))
	for i, resource := range resources {
		if statements[i].Resource == "" {
			statements[i] = statement
			statements[i].Resource = resource
		}
	}
	return statements, nil
}

func MakeStatementForPolicyTypeOrDie(typ string, resources []string) model.Statements {
	statements, err := MakeStatementForPolicyType(typ, resources)
	if err != nil {
		panic(err)
	}
	return statements
}

func SetupBaseGroups(ctx context.Context, authService Service, ts time.Time) error {
	var err error

	err = createGroups(ctx, authService, []*model.Group{
		{CreatedAt: ts, DisplayName: AdminsGroup},
		{CreatedAt: ts, DisplayName: SuperUsersGroup},
		{CreatedAt: ts, DisplayName: DevelopersGroup},
		{CreatedAt: ts, DisplayName: ViewersGroup},
	})
	if err != nil {
		return err
	}

	all := []string{permissions.All}

	err = createPolicies(ctx, authService, []*model.Policy{
		{
			CreatedAt:   ts,
			DisplayName: "FSFullAccess",
			Statement:   MakeStatementForPolicyTypeOrDie("FSFullAccess", all),
		},
		{
			CreatedAt:   ts,
			DisplayName: "FSReadWriteAll",
			Statement:   MakeStatementForPolicyTypeOrDie("FSReadWrite", all),
		},
		{
			CreatedAt:   ts,
			DisplayName: "FSReadAll",
			Statement:   MakeStatementForPolicyTypeOrDie("FSRead", all),
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
			Statement: model.Statements{
				{
					Action: []string{
						"ci:Read*",
						"retention:Get*",
						"branches:Get*",
						"fs:ReadConfig",
					},
					Resource: permissions.All,
					Effect:   model.StatementEffectAllow,
				},
			},
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
			Statement: MakeStatementForPolicyTypeOrDie(
				"AuthManageOwnCredentials",
				[]string{permissions.UserArn("${user}")},
			),
		},
	})
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

func SetupAdminUser(ctx context.Context, authService Service, superuser *model.SuperuserConfiguration) (*model.Credential, error) {
	now := time.Now()

	// Setup the basic groups and policies
	err := SetupBaseGroups(ctx, authService, now)
	if err != nil {
		return nil, err
	}

	return AddAdminUser(ctx, authService, superuser)
}

func AddAdminUser(ctx context.Context, authService Service, user *model.SuperuserConfiguration) (*model.Credential, error) {
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

func CreateInitialAdminUser(ctx context.Context, authService Service, metadataManger MetadataManager, username string) (*model.Credential, error) {
	return CreateInitialAdminUserWithKeys(ctx, authService, metadataManger, username, nil, nil)
}

func CreateInitialAdminUserWithKeys(ctx context.Context, authService Service, metadataManger MetadataManager, username string, accessKeyID *string, secretAccessKey *string) (*model.Credential, error) {
	adminUser := &model.SuperuserConfiguration{User: model.User{
		CreatedAt: time.Now(),
		Username:  username,
	}}
	if accessKeyID != nil && secretAccessKey != nil {
		adminUser.AccessKeyID = *accessKeyID
		adminUser.SecretAccessKey = *secretAccessKey
	}
	// create first admin user
	cred, err := SetupAdminUser(ctx, authService, adminUser)
	if err != nil {
		return nil, err
	}

	// update setup timestamp
	if err := metadataManger.UpdateSetupTimestamp(ctx, time.Now()); err != nil {
		logging.Default().WithError(err).Error("Failed the update setup timestamp")
	}
	return cred, err
}
