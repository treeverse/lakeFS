package permissions

import (
	"errors"
	"fmt"
	"strings"
)

var (
	ErrInvalidAction      = errors.New("invalid action")
	ErrInvalidServiceName = errors.New("invalid service name")
)

const (
	ReadRepositoryAction     = "fs:ReadRepository"
	CreateRepositoryAction   = "fs:CreateRepository"
	AttachStorageNamespace   = "fs:AttachStorageNamespace"
	DeleteRepositoryAction   = "fs:DeleteRepository"
	ListRepositoriesAction   = "fs:ListRepositories"
	ReadObjectAction         = "fs:ReadObject"
	WriteObjectAction        = "fs:WriteObject"
	DeleteObjectAction       = "fs:DeleteObject"
	ListObjectsAction        = "fs:ListObjects"
	CreateCommitAction       = "fs:CreateCommit"
	ReadCommitAction         = "fs:ReadCommit"
	ListCommitsAction        = "fs:ListCommits"
	CreateBranchAction       = "fs:CreateBranch"
	DeleteBranchAction       = "fs:DeleteBranch"
	ReadBranchAction         = "fs:ReadBranch"
	RevertBranchAction       = "fs:RevertBranch"
	ListBranchesAction       = "fs:ListBranches"
	CreateTagAction          = "fs:CreateTag"
	DeleteTagAction          = "fs:DeleteTag"
	ReadTagAction            = "fs:ReadTag"
	ListTagsAction           = "fs:ListTags"
	ReadStorageConfiguration = "fs:ReadConfig"

	ReadUserAction          = "auth:ReadUser"
	CreateUserAction        = "auth:CreateUser"
	DeleteUserAction        = "auth:DeleteUser"
	ListUsersAction         = "auth:ListUsers"
	ReadGroupAction         = "auth:ReadGroup"
	CreateGroupAction       = "auth:CreateGroup"
	DeleteGroupAction       = "auth:DeleteGroup"
	ListGroupsAction        = "auth:ListGroups"
	AddGroupMemberAction    = "auth:AddGroupMember"
	RemoveGroupMemberAction = "auth:RemoveGroupMember"
	ReadPolicyAction        = "auth:ReadPolicy"
	CreatePolicyAction      = "auth:CreatePolicy"
	UpdatePolicyAction      = "auth:UpdatePolicy"
	DeletePolicyAction      = "auth:DeletePolicy"
	ListPoliciesAction      = "auth:ListPolicies"
	AttachPolicyAction      = "auth:AttachPolicy"
	DetachPolicyAction      = "auth:DetachPolicy"
	ReadCredentialsAction   = "auth:ReadCredentials"
	CreateCredentialsAction = "auth:CreateCredentials"
	DeleteCredentialsAction = "auth:DeleteCredentials"
	ListCredentialsAction   = "auth:ListCredentials"

	ReadActionsAction = "ci:ReadAction"

	PrepareGarbageCollectionCommitsAction = "retention:PrepareGarbageCollectionCommits"
	GetGarbageCollectionRulesAction       = "retention:GetGarbageCollectionRules"
	SetGarbageCollectionRulesAction       = "retention:SetGarbageCollectionRules"
)

var serviceSet = map[string]struct{}{
	"fs":        {},
	"auth":      {},
	"ci":        {},
	"retention": {},
}

func IsValidAction(name string) error {
	parts := strings.Split(name, ":")
	const actionParts = 2
	if len(parts) != actionParts {
		return fmt.Errorf("%s: %w", name, ErrInvalidAction)
	}
	if _, ok := serviceSet[parts[0]]; !ok {
		return fmt.Errorf("%s: %w", name, ErrInvalidServiceName)
	}
	return nil
}
