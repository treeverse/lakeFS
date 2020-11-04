package auth

import (
	"fmt"
	"time"

	"github.com/treeverse/lakefs/auth/model"
	"github.com/treeverse/lakefs/permissions"
)

func createGroups(authService Service, groups []*model.Group) error {
	for _, group := range groups {
		err := authService.CreateGroup(group)
		if err != nil {
			return err
		}
	}
	return nil
}

func createPolicies(authService Service, policies []*model.Policy) error {
	for _, policy := range policies {
		err := authService.WritePolicy(policy)
		if err != nil {
			return err
		}
	}
	return nil
}

func attachPolicies(authService Service, groupID string, policyIDs []string) error {
	for _, policyID := range policyIDs {
		err := authService.AttachPolicyToGroup(policyID, groupID)
		if err != nil {
			return err
		}
	}
	return nil
}

func SetupBaseGroups(authService Service, ts time.Time) error {
	var err error

	err = createGroups(authService, []*model.Group{
		{CreatedAt: ts, DisplayName: "Admins"},
		{CreatedAt: ts, DisplayName: "SuperUsers"},
		{CreatedAt: ts, DisplayName: "Developers"},
		{CreatedAt: ts, DisplayName: "Viewers"},
	})
	if err != nil {
		return err
	}

	err = createPolicies(authService, []*model.Policy{
		{
			CreatedAt:   ts,
			DisplayName: "FSFullAccess",
			Statement: model.Statements{
				{
					Action: []string{
						"fs:*",
					},
					Resource: permissions.All,
					Effect:   model.StatementEffectAllow,
				},
			},
		},
		{
			CreatedAt:   ts,
			DisplayName: "FSReadWriteAll",
			Statement: model.Statements{
				{
					Action: []string{
						permissions.ListRepositoriesAction,
						permissions.ReadRepositoryAction,
						permissions.ReadCommitAction,
						permissions.ListBranchesAction,
						permissions.ListObjectsAction,
						permissions.ReadObjectAction,
						permissions.WriteObjectAction,
						permissions.DeleteObjectAction,
						permissions.RevertBranchAction,
						permissions.ReadBranchAction,
						permissions.CreateBranchAction,
						permissions.DeleteBranchAction,
						permissions.CreateCommitAction,
					},
					Resource: permissions.All,
					Effect:   model.StatementEffectAllow,
				},
			},
		},
		{
			CreatedAt:   ts,
			DisplayName: "FSReadAll",
			Statement: model.Statements{
				{
					Action: []string{
						"fs:List*",
						"fs:Read*",
					},
					Resource: permissions.All,
					Effect:   model.StatementEffectAllow,
				},
			},
		},
		{
			CreatedAt:   ts,
			DisplayName: "RepoManagementFullAccess",
			Statement: model.Statements{
				{
					Action: []string{
						"retention:*",
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
						"retention:Get*",
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
			Statement: model.Statements{
				{
					Action: []string{
						permissions.CreateCredentialsAction,
						permissions.DeleteCredentialsAction,
						permissions.ListCredentialsAction,
						permissions.ReadCredentialsAction,
					},
					Resource: permissions.UserArn("${user}"),
					Effect:   model.StatementEffectAllow,
				},
			},
		},
	})
	if err != nil {
		return err
	}

	err = attachPolicies(authService, "Admins", []string{"FSFullAccess", "AuthFullAccess", "RepoManagementFullAccess"})
	if err != nil {
		return err
	}
	err = attachPolicies(authService, "SuperUsers", []string{"FSFullAccess", "AuthManageOwnCredentials", "RepoManagementReadAll"})
	if err != nil {
		return err
	}
	err = attachPolicies(authService, "Developers", []string{"FSReadWriteAll", "AuthManageOwnCredentials", "RepoManagementReadAll"})
	if err != nil {
		return err
	}
	err = attachPolicies(authService, "Viewers", []string{"FSReadAll", "AuthManageOwnCredentials"})
	if err != nil {
		return err
	}

	return nil
}

func SetupAdminUser(authService Service, user *model.User) (*model.Credential, error) {
	now := time.Now()

	// Setup the basic groups and policies
	err := SetupBaseGroups(authService, now)
	if err != nil {
		return nil, err
	}

	return AddAdminUser(authService, user)
}

func AddAdminUser(authService Service, user *model.User) (*model.Credential, error) {
	const adminGroupName = "Admins"

	// verify admin group exists
	_, err := authService.GetGroup(adminGroupName)
	if err != nil {
		return nil, fmt.Errorf("admin group - %w", err)
	}

	// create admin user
	err = authService.CreateUser(user)
	if err != nil {
		return nil, fmt.Errorf("create user - %w", err)
	}
	err = authService.AddUserToGroup(user.Username, adminGroupName)
	if err != nil {
		return nil, fmt.Errorf("add user to group - %w", err)
	}

	// Generate and return a key pair
	creds, err := authService.CreateCredentials(user.Username)
	if err != nil {
		return nil, fmt.Errorf("create credentials for %s %w", user.Username, err)
	}
	return creds, nil
}
