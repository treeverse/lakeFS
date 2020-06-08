package permissions

import (
	"errors"
	"fmt"
	"strings"
)

type Action string

var (
	ErrInvalidAction      = errors.New("invalid action")
	ErrInvalidServiceName = errors.New("invalid service name")
)

const (
	ReadRepositoryAction   Action = "fs:ReadRepository"
	CreateRepositoryAction Action = "fs:CreateRepository"
	DeleteRepositoryAction Action = "fs:DeleteRepository"
	ListRepositoriesAction Action = "fs:ListRepositories"
	ReadObjectAction       Action = "fs:ReadObject"
	WriteObjectAction      Action = "fs:WriteObject"
	DeleteObjectAction     Action = "fs:DeleteObject"
	ListObjectsAction      Action = "fs:ListObjects"
	CreateCommitAction     Action = "fs:CreateCommit"
	ReadCommitAction       Action = "fs:ReadCommit"
	CreateBranchAction     Action = "fs:CreateBranch"
	DeleteBranchAction     Action = "fs:DeleteBranch"
	ReadBranchAction       Action = "fs:ReadBranch"
	RevertBranchAction     Action = "fs:RevertBranch"
	ListBranchesAction     Action = "fs:ListBranches"

	ReadUserAction          Action = "auth:ReadUser"
	CreateUserAction        Action = "auth:CreateUser"
	DeleteUserAction        Action = "auth:DeleteUser"
	ListUsersAction         Action = "auth:ListUsers"
	ReadGroupAction         Action = "auth:ReadGroup"
	CreateGroupAction       Action = "auth:CreateGroup"
	DeleteGroupAction       Action = "auth:DeleteGroup"
	ListGroupsAction        Action = "auth:ListGroups"
	AddGroupMemberAction    Action = "auth:AddGroupMember"
	RemoveGroupMemberAction Action = "auth:RemoveGroupMember"
	ReadPolicyAction        Action = "auth:ReadPolicy"
	CreatePolicyAction      Action = "auth:CreatePolicy"
	DeletePolicyAction      Action = "auth:DeletePolicy"
	ListPoliciesAction      Action = "auth:ListPolicies"
	AttachPolicyAction      Action = "auth:AttachPolicy"
	DetachPolicyAction      Action = "auth:DetachPolicy"
	ReadCredentialsAction   Action = "auth:ReadCredentials"
	CreateCredentialsAction Action = "auth:CreateCredentials"
	DeleteCredentialsAction Action = "auth:DeleteCredentials"
	ListCredentialsAction   Action = "auth:ListCredentials"
)

var serviceSet = map[string]struct{}{
	"fs":   {},
	"auth": {},
}

func IsValidAction(name string) error {
	parts := strings.Split(name, ":")
	if len(parts) != 2 {
		return fmt.Errorf("%s: %w", name, ErrInvalidAction)
	}
	if _, ok := serviceSet[parts[0]]; !ok {
		return fmt.Errorf("%s: %w", name, ErrInvalidServiceName)
	}
	return nil
}
