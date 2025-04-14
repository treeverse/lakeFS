package permissions

func CreatePresignMultipartUploadPermissions(repository string, path string) Node {
	return Node{
		Permission: Permission{
			Action:   WriteObjectAction,
			Resource: ObjectArn(repository, path),
		},
	}
}

func AbortPresignMultipartUploadPermissions(repository string, path string) Node {
	return Node{
		Permission: Permission{
			Action:   WriteObjectAction,
			Resource: ObjectArn(repository, path),
		},
	}
}

func CompletePresignMultipartUploadPermissions(repository string, path string) Node {
	return Node{
		Permission: Permission{
			Action:   WriteObjectAction,
			Resource: ObjectArn(repository, path),
		},
	}
}

func ListGroupPermissions() Node {
	return Node{
		Permission: Permission{
			Action:   ListGroupsAction,
			Resource: All,
		},
	}
}

func CreateGroupPermissions(groupID string) Node {
	return Node{
		Permission: Permission{
			Action:   CreateGroupAction,
			Resource: GroupArn(groupID),
		},
	}
}

func DeleteGroupPermissions(groupID string) Node {
	return Node{
		Permission: Permission{
			Action:   DeleteGroupAction,
			Resource: GroupArn(groupID),
		},
	}
}

func GetGroupPermissions(groupID string) Node {
	return Node{
		Permission: Permission{
			Action:   ReadGroupAction,
			Resource: GroupArn(groupID),
		},
	}
}

func GetGroupAclPermissions(groupID string, aclPolicyName string) Node {
	return Node{
		Type: NodeTypeAnd,
		Nodes: []Node{
			{
				Permission: Permission{
					Action:   ReadGroupAction,
					Resource: GroupArn(groupID),
				},
			},
			{
				Permission: Permission{
					Action:   ReadPolicyAction,
					Resource: PolicyArn(aclPolicyName),
				},
			},
		},
	}
}

func SetGroupAclPermissions(groupID string, aclPolicyName string) Node {
	return Node{
		Type: NodeTypeAnd,
		Nodes: []Node{
			{
				Permission: Permission{
					Action:   ReadGroupAction,
					Resource: GroupArn(groupID),
				},
			},
			{
				Permission: Permission{
					Action:   AttachPolicyAction,
					Resource: PolicyArn(aclPolicyName),
				},
			},
			{
				Permission: Permission{
					Action:   UpdatePolicyAction,
					Resource: PolicyArn(aclPolicyName),
				},
			},
		},
	}
}

func ListGroupUsersPermissions(groupID string) Node {
	return Node{
		Permission: Permission{
			Action:   ReadGroupAction,
			Resource: GroupArn(groupID),
		},
	}
}

func RemoveUserFromGroupPermissions(groupID string) Node {
	return Node{
		Permission: Permission{
			Action:   RemoveGroupMemberAction,
			Resource: GroupArn(groupID),
		},
	}
}

func AddUserToGroupPermissions(groupID string) Node {
	return Node{
		Permission: Permission{
			Action:   AddGroupMemberAction,
			Resource: GroupArn(groupID),
		},
	}
}

func ListGroupPoliciesPermissions(groupID string) Node {
	return Node{
		Permission: Permission{
			Action:   ReadGroupAction,
			Resource: GroupArn(groupID),
		},
	}
}

func DetachPolicyFromGroupPermissions(groupID string) Node {
	return Node{
		Permission: Permission{
			Action:   DetachPolicyAction,
			Resource: GroupArn(groupID),
		},
	}
}

func AttachPolicyToGroupPermissions(groupID string) Node {
	return Node{
		Permission: Permission{
			Action:   AttachPolicyAction,
			Resource: GroupArn(groupID),
		},
	}
}

func ListPoliciesPermissions() Node {
	return Node{
		Permission: Permission{
			Action:   ListPoliciesAction,
			Resource: All,
		},
	}
}

func CreatePolicyPermissions(policyID string) Node {
	return Node{
		Permission: Permission{
			Action:   CreatePolicyAction,
			Resource: PolicyArn(policyID),
		},
	}
}

func DeletePolicyPermissions(policyID string) Node {
	return Node{
		Permission: Permission{
			Action:   DeletePolicyAction,
			Resource: PolicyArn(policyID),
		},
	}
}

func GetPolicyPermissions(policyID string) Node {
	return Node{
		Permission: Permission{
			Action:   ReadPolicyAction,
			Resource: PolicyArn(policyID),
		},
	}
}

func UpdatePolicyPermissions(policyID string) Node {
	return Node{
		Permission: Permission{
			Action:   UpdatePolicyAction,
			Resource: PolicyArn(policyID),
		},
	}
}

func ListUsersPermissions() Node {
	return Node{
		Permission: Permission{
			Action:   ListUsersAction,
			Resource: All,
		},
	}
}

func CreateUserPermissions(username string) Node {
	return Node{
		Permission: Permission{
			Action:   CreateUserAction,
			Resource: UserArn(username),
		},
	}
}

func GetUserPermissions(userID string) Node {
	return Node{
		Permission: Permission{
			Action:   ReadUserAction,
			Resource: UserArn(userID),
		},
	}
}

func ListUserCredentialsPermissions(userID string) Node {
	return Node{
		Permission: Permission{
			Action:   ListCredentialsAction,
			Resource: UserArn(userID),
		},
	}
}

func CreateCredentialsPermissions(userID string) Node {
	return Node{
		Permission: Permission{
			Action:   CreateCredentialsAction,
			Resource: UserArn(userID),
		},
	}
}

func DeleteCredentialsPermissions(userID string) Node {
	return Node{
		Permission: Permission{
			Action:   DeleteCredentialsAction,
			Resource: UserArn(userID),
		},
	}
}

func GetCredentialsForUserPermissions(userID string) Node {
	return Node{
		Permission: Permission{
			Action:   ReadCredentialsAction,
			Resource: UserArn(userID),
		},
	}
}

func ListUserGroupsPermissions(userID string) Node {
	return Node{
		Permission: Permission{
			Action:   ReadUserAction,
			Resource: UserArn(userID),
		},
	}
}

func ListUserPoliciesPermissions(userID string) Node {
	return Node{
		Permission: Permission{
			Action:   ReadUserAction,
			Resource: UserArn(userID),
		},
	}
}

func DetachPolicyFromUserPermissions(userID string) Node {
	return Node{
		Permission: Permission{
			Action:   DetachPolicyAction,
			Resource: UserArn(userID),
		},
	}
}

func AttachPolicyToUserPermissions(userID string) Node {
	return Node{
		Permission: Permission{
			Action:   AttachPolicyAction,
			Resource: UserArn(userID),
		},
	}
}

func CreateRepoPermissions(repoName string, storageNamespace string) Node {
	return Node{
		Type: NodeTypeAnd,
		Nodes: []Node{
			{
				Permission: Permission{
					Action:   CreateRepositoryAction,
					Resource: RepoArn(repoName),
				},
			},
			{
				Permission: Permission{
					Action:   AttachStorageNamespaceAction,
					Resource: StorageNamespace(storageNamespace),
				},
			},
		},
	}
}

func DeleteRepoPermissions(repository string) Node {
	return Node{
		Permission: Permission{
			Action:   DeleteRepositoryAction,
			Resource: RepoArn(repository),
		},
	}
}

func GetRepoPermissions(repository string) Node {
	return Node{
		Permission: Permission{
			Action:   ReadRepositoryAction,
			Resource: RepoArn(repository),
		},
	}
}

func GetRepoMetadataPermissions(repository string) Node {
	return Node{
		Permission: Permission{
			Action:   ReadRepositoryAction,
			Resource: RepoArn(repository),
		},
	}
}

func SetRepoMetadataPermissions(repository string) Node {
	return Node{
		Permission: Permission{
			Action:   UpdateRepositoryAction,
			Resource: RepoArn(repository),
		},
	}
}

func DeleteRepoMetadataPermissions(repository string) Node {
	return Node{
		Permission: Permission{
			Action:   UpdateRepositoryAction,
			Resource: RepoArn(repository),
		},
	}
}

func DeleteUserPermissions(userID string) Node {
	return Node{
		Permission: Permission{
			Action:   DeleteUserAction,
			Resource: UserArn(userID),
		},
	}
}

func GetBranchProtectionRulesPermissions(repository string) Node {
	return Node{
		Permission: Permission{
			Action:   GetBranchProtectionRulesAction,
			Resource: RepoArn(repository),
		},
	}
}

func SetBranchProtectionRulesPermissions(repository string) Node {
	return Node{
		Permission: Permission{
			Action:   SetBranchProtectionRulesAction,
			Resource: RepoArn(repository),
		},
	}
}

func DeleteGCRulesPermissions(repository string) Node {
	return Node{
		Permission: Permission{
			Action:   SetGarbageCollectionRulesAction,
			Resource: RepoArn(repository),
		},
	}
}

func GetGCRulesPermissions(repository string) Node {
	return Node{
		Permission: Permission{
			Action:   GetGarbageCollectionRulesAction,
			Resource: RepoArn(repository),
		},
	}
}

func SetGCRulesPermissions(repository string) Node {
	return Node{
		Permission: Permission{
			Action:   SetGarbageCollectionRulesAction,
			Resource: RepoArn(repository),
		},
	}
}

func ListRepositoryRunsPermissions(repository string) Node {
	return Node{
		Permission: Permission{
			Action:   ReadActionsAction,
			Resource: RepoArn(repository),
		},
	}
}

func ActionsGetRunPermissions(repository string) Node {
	return Node{
		Permission: Permission{
			Action:   ReadActionsAction,
			Resource: RepoArn(repository),
		},
	}
}

func ActionsListRunHooksPermissions(repository string) Node {
	return Node{
		Permission: Permission{
			Action:   ReadActionsAction,
			Resource: RepoArn(repository),
		},
	}
}

func ActionsRunHookOutputPermissions(repository string) Node {
	return Node{
		Permission: Permission{
			Action:   ReadActionsAction,
			Resource: RepoArn(repository),
		},
	}
}

func ListBranchesPermissions(repository string) Node {
	return Node{
		Permission: Permission{
			Action:   ListBranchesAction,
			Resource: RepoArn(repository),
		},
	}
}

func CreateBranchPermissions(repository string, branchName string) Node {
	return Node{
		Permission: Permission{
			Action:   CreateBranchAction,
			Resource: BranchArn(repository, branchName),
		},
	}
}

func DeleteBranchPermissions(repository string, branch string) Node {
	return Node{
		Permission: Permission{
			Action:   DeleteBranchAction,
			Resource: BranchArn(repository, branch),
		},
	}
}

func GetBranchPermissions(repository string, branch string) Node {
	return Node{
		Permission: Permission{
			Action:   ReadBranchAction,
			Resource: BranchArn(repository, branch),
		},
	}
}

func ResetBranchPermissions(repository string, branch string) Node {
	return Node{
		Permission: Permission{
			Action:   RevertBranchAction,
			Resource: BranchArn(repository, branch),
		},
	}
}

func HardResetBranchPermissions(repository string, branch string) Node {
	return Node{
		Permission: Permission{
			// TODO(ozkatz): Can we have another action here?
			Action:   RevertBranchAction,
			Resource: BranchArn(repository, branch),
		},
	}
}

func ImportPermissions(repository string, branch string) Node {
	return Node{
		Type: NodeTypeAnd,
		Nodes: []Node{
			{
				Permission: Permission{
					Action:   WriteObjectAction,
					Resource: BranchArn(repository, branch),
				},
			},
			{
				Permission: Permission{
					Action:   CreateCommitAction,
					Resource: BranchArn(repository, branch),
				},
			},
		},
	}
}

func ImportStatusPermissions(repository string, branch string) Node {
	return Node{
		Permission: Permission{
			Action:   ReadBranchAction,
			Resource: BranchArn(repository, branch),
		},
	}
}

func CancelImportPermissions(repository string, branch string) Node {
	return Node{
		Permission: Permission{
			Action:   ImportCancelAction,
			Resource: BranchArn(repository, branch),
		},
	}
}

func CreateCommitPermissions(repository string, branch string) Node {
	return Node{
		Permission: Permission{
			Action:   CreateCommitAction,
			Resource: BranchArn(repository, branch),
		},
	}
}

func CreateCommitRecordPermissions(repository string) Node {
	return Node{
		Permission: Permission{
			Action:   CreateCommitAction,
			Resource: RepoArn(repository),
		},
	}
}

func DiffWorkspacePermissions(repository string) Node {
	return Node{
		Permission: Permission{
			Action:   ListObjectsAction,
			Resource: RepoArn(repository),
		},
	}
}

func DeleteObjectPermissions(repository string, path string) Node {
	return Node{
		Permission: Permission{
			Action:   DeleteObjectAction,
			Resource: ObjectArn(repository, path),
		},
	}
}

func PutObjectPreflightPermissions(repository string, path string) Node {
	return Node{
		Permission: Permission{
			Action:   WriteObjectAction,
			Resource: ObjectArn(repository, path),
		},
	}
}

func PrepareGarbageCollectionUncommittedPermissions(repository string) Node {
	return Node{
		Permission: Permission{
			Action:   PrepareGarbageCollectionUncommittedAction,
			Resource: RepoArn(repository),
		},
	}
}

func DeleteObjectsPermissions(repository string, objectPath string) Node {
	return Node{
		Permission: Permission{
			Action:   DeleteObjectAction,
			Resource: ObjectArn(repository, objectPath),
		},
	}
}

func GeneratePhysicalAddressPermissions(repository string, path string) Node {
	return Node{
		Permission: Permission{
			Action:   WriteObjectAction,
			Resource: ObjectArn(repository, path),
		},
	}
}

func StageObjectPermissions(repository string, path string) Node {
	return Node{
		Permission: Permission{
			Action:   WriteObjectAction,
			Resource: ObjectArn(repository, path),
		},
	}
}

func ListReposPermissions() Node {
	return Node{
		Permission: Permission{
			Action:   ListRepositoriesAction,
			Resource: All,
		},
	}
}

func PutObjectPermissions(repository string, path string) Node {
	return Node{
		Permission: Permission{
			Action:   WriteObjectAction,
			Resource: ObjectArn(repository, path),
		},
	}
}

func CopyObjectPermissions(repository string, srcPath string, destPath string) Node {
	return Node{
		Type: NodeTypeAnd,
		Nodes: []Node{
			{
				Permission: Permission{
					Action:   ReadObjectAction,
					Resource: ObjectArn(repository, srcPath),
				},
			},
			{
				Permission: Permission{
					Action:   WriteObjectAction,
					Resource: ObjectArn(repository, destPath),
				},
			},
		},
	}
}

func RevertBranchPermissions(repository string, branch string) Node {
	return Node{
		Permission: Permission{
			Action:   RevertBranchAction,
			Resource: BranchArn(repository, branch),
		},
	}
}

func CherryPickPermissions(repository string, branch string) Node {
	return Node{
		Type: NodeTypeAnd,
		Nodes: []Node{
			{
				Permission: Permission{
					Action:   CreateCommitAction,
					Resource: BranchArn(repository, branch),
				},
			},
			{
				Permission: Permission{
					Action:   ReadCommitAction,
					Resource: RepoArn(repository),
				},
			},
		},
	}
}

func GetCommitPermissions(repository string) Node {
	return Node{
		Permission: Permission{
			Action:   ReadCommitAction,
			Resource: RepoArn(repository),
		},
	}
}

func SetGcCollectionRulesPreflightPermissions(repository string) Node {
	return Node{
		Permission: Permission{
			Action:   SetGarbageCollectionRulesAction,
			Resource: RepoArn(repository),
		},
	}
}

func PrepareGarbageCollectionCommitsPermissions(repository string) Node {
	return Node{
		Permission: Permission{
			Action:   PrepareGarbageCollectionCommitsAction,
			Resource: RepoArn(repository),
		},
	}
}

func DeleteBranchProtectionRulePermissions(repository string) Node {
	return Node{
		Permission: Permission{
			Action:   SetBranchProtectionRulesAction,
			Resource: RepoArn(repository),
		},
	}
}

func CreateBranchProtectionRulePreflightPermissions(repository string) Node {
	return Node{
		Permission: Permission{
			Action:   SetBranchProtectionRulesAction,
			Resource: RepoArn(repository),
		},
	}
}

func CreateBranchProtectionRulePermissions(repository string) Node {
	return Node{
		Permission: Permission{
			Action:   SetBranchProtectionRulesAction,
			Resource: RepoArn(repository),
		},
	}
}

func MetadataGetMetarangePermissions(repository string) Node {
	return Node{
		Type: NodeTypeAnd,
		Nodes: []Node{
			{
				Permission: Permission{
					Action:   ListObjectsAction,
					Resource: RepoArn(repository),
				},
			},
			{
				Permission: Permission{
					Action:   ReadRepositoryAction,
					Resource: RepoArn(repository),
				},
			},
		},
	}
}

func MetadataGetRangePermissions(repository string) Node {
	return Node{
		Type: NodeTypeAnd,
		Nodes: []Node{
			{
				Permission: Permission{
					Action:   ListObjectsAction,
					Resource: RepoArn(repository),
				},
			},
			{
				Permission: Permission{
					Action:   ReadRepositoryAction,
					Resource: RepoArn(repository),
				},
			},
		},
	}
}

func DumpRepositoryRefsPermissions(repository string) Node {
	return Node{
		Type: NodeTypeAnd,
		Nodes: []Node{
			{
				Permission: Permission{
					Action:   ListTagsAction,
					Resource: RepoArn(repository),
				},
			},
			{
				Permission: Permission{
					Action:   ListBranchesAction,
					Resource: RepoArn(repository),
				},
			},
			{
				Permission: Permission{
					Action:   ListCommitsAction,
					Resource: RepoArn(repository),
				},
			},
		},
	}
}

func RestoreRepositoryRefsPermissions(repository string) Node {
	return Node{
		Type: NodeTypeAnd,
		Nodes: []Node{
			{
				Permission: Permission{
					Action:   CreateTagAction,
					Resource: RepoArn(repository),
				},
			},
			{
				Permission: Permission{
					Action:   CreateBranchAction,
					Resource: RepoArn(repository),
				},
			},
			{
				Permission: Permission{
					Action:   CreateCommitAction,
					Resource: RepoArn(repository),
				},
			},
		},
	}
}

func DumpRepositoryPermissions(repository string) Node {
	return Node{
		Type: NodeTypeAnd,
		Nodes: []Node{
			{
				Permission: Permission{
					Action:   ListTagsAction,
					Resource: RepoArn(repository),
				},
			},
			{
				Permission: Permission{
					Action:   ListBranchesAction,
					Resource: RepoArn(repository),
				},
			},
			{
				Permission: Permission{
					Action:   ListCommitsAction,
					Resource: RepoArn(repository),
				},
			},
		},
	}
}

func DumpRepoStatusPermissions(repository string) Node {
	return Node{
		Type: NodeTypeAnd,
		Nodes: []Node{
			{
				Permission: Permission{
					Action:   ListTagsAction,
					Resource: RepoArn(repository),
				},
			},
			{
				Permission: Permission{
					Action:   ListBranchesAction,
					Resource: RepoArn(repository),
				},
			},
			{
				Permission: Permission{
					Action:   ListCommitsAction,
					Resource: RepoArn(repository),
				},
			},
		},
	}
}

func RestoreRepositoryPermissions(repository string) Node {
	return Node{
		Type: NodeTypeAnd,
		Nodes: []Node{
			{
				Permission: Permission{
					Action:   CreateTagAction,
					Resource: RepoArn(repository),
				},
			},
			{
				Permission: Permission{
					Action:   CreateBranchAction,
					Resource: RepoArn(repository),
				},
			},
			{
				Permission: Permission{
					Action:   CreateCommitAction,
					Resource: RepoArn(repository),
				},
			},
		},
	}
}

func RestoreRepositoryStatusPermissions(repository string) Node {
	return Node{
		Type: NodeTypeAnd,
		Nodes: []Node{
			{
				Permission: Permission{
					Action:   CreateTagAction,
					Resource: RepoArn(repository),
				},
			},
			{
				Permission: Permission{
					Action:   CreateBranchAction,
					Resource: RepoArn(repository),
				},
			},
			{
				Permission: Permission{
					Action:   CreateCommitAction,
					Resource: RepoArn(repository),
				},
			},
		},
	}
}

func CreateSymlinkPermissions(repository string, branch string) Node {
	return Node{
		Permission: Permission{
			Action:   WriteObjectAction,
			Resource: ObjectArn(repository, branch),
		},
	}
}

func DiffRefsPermissions(repository string) Node {
	return Node{
		Permission: Permission{
			Action:   ListObjectsAction,
			Resource: RepoArn(repository),
		},
	}
}

func GetBranchCommitLogPermissions(repository string, ref string) Node {
	return Node{
		Permission: Permission{
			Action:   ReadBranchAction,
			Resource: BranchArn(repository, ref),
		},
	}
}

func HeadObjectPermissions(repository string, path string) Node {
	return Node{
		Permission: Permission{
			Action:   ReadObjectAction,
			Resource: ObjectArn(repository, path),
		},
	}
}

func GetMetadataObjectPermissions(repository string) Node {
	return Node{
		Type: NodeTypeAnd,
		Nodes: []Node{
			{
				Permission: Permission{
					Action:   ListObjectsAction,
					Resource: RepoArn(repository),
				},
			},
			{
				Permission: Permission{
					Action:   ReadRepositoryAction,
					Resource: RepoArn(repository),
				},
			},
		},
	}
}

func GetObjectPermissions(repository string, path string) Node {
	return Node{
		Permission: Permission{
			Action:   ReadObjectAction,
			Resource: ObjectArn(repository, path),
		},
	}
}

func ListObjectsPermissions(repository string) Node {
	return Node{
		Permission: Permission{
			Action:   ListObjectsAction,
			Resource: RepoArn(repository),
		},
	}
}

func StartObjectPermissions(repository string, path string) Node {
	return Node{
		Permission: Permission{
			Action:   ReadObjectAction,
			Resource: ObjectArn(repository, path),
		},
	}
}

func UpdateObjectUserMetadataPermissions(repository string, path string) Node {
	return Node{
		Permission: Permission{
			Action:   WriteObjectAction,
			Resource: ObjectArn(repository, path),
		},
	}
}

func ObjectUnderlyingPropertiesPermissions(repository string, path string) Node {
	return Node{
		Permission: Permission{
			Action:   ReadObjectAction,
			Resource: ObjectArn(repository, path),
		},
	}
}

func MergeBranchesPermissions(repository string, destinationBranch string) Node {
	return Node{
		Permission: Permission{
			Action:   CreateCommitAction,
			Resource: BranchArn(repository, destinationBranch),
		},
	}
}

func FindMergeBasePermissions(repository string) Node {
	return Node{
		Permission: Permission{
			Action:   ListCommitsAction,
			Resource: RepoArn(repository),
		},
	}
}

func ListTagsPermissions(repository string) Node {
	return Node{
		Permission: Permission{
			Action:   ListTagsAction,
			Resource: RepoArn(repository),
		},
	}
}

func CreateTagPermissions(repository string, tagID string) Node {
	return Node{
		Permission: Permission{
			Action:   CreateTagAction,
			Resource: TagArn(repository, tagID),
		},
	}
}

func DeleteTagPermissions(repository string, tag string) Node {
	return Node{
		Permission: Permission{
			Action:   DeleteTagAction,
			Resource: TagArn(repository, tag),
		},
	}
}

func GetTagPermissions(repository string, tag string) Node {
	return Node{
		Permission: Permission{
			Action:   ReadTagAction,
			Resource: TagArn(repository, tag),
		},
	}
}

func ListPullRequestsPermissions(repository string) Node {
	return Node{
		Permission: Permission{
			Action:   ListPullRequestsAction,
			Resource: RepoArn(repository),
		},
	}
}

func CreatePullRequestPermissions(repository string) Node {
	return Node{
		Permission: Permission{
			Action:   WritePullRequestAction,
			Resource: RepoArn(repository),
		},
	}
}

func GetPullRequestPermissions(repository string) Node {
	return Node{
		Permission: Permission{
			Action:   ReadPullRequestAction,
			Resource: RepoArn(repository),
		},
	}
}

func UpdatePullRequestPermissions(repository string) Node {
	return Node{
		Permission: Permission{
			Action:   WritePullRequestAction,
			Resource: RepoArn(repository),
		},
	}
}

func MergePullRequestsPermissions(repository string, destination string) Node {
	return Node{
		Type: NodeTypeAnd,
		Nodes: []Node{
			{
				Permission: Permission{
					Action:   CreateCommitAction,
					Resource: BranchArn(repository, destination),
				},
			},
			{
				Permission: Permission{
					Action:   WritePullRequestAction,
					Resource: RepoArn(repository),
				},
			},
		},
	}
}

func CreateUserExternalPrincipalPermissions(userID string) Node {
	return Node{
		Permission: Permission{
			Action:   CreateUserExternalPrincipalAction,
			Resource: UserArn(userID),
		},
	}
}

func DeleteUserExternalPrincipalPermissions(userID string) Node {
	return Node{
		Permission: Permission{
			Action:   DeleteUserExternalPrincipalAction,
			Resource: UserArn(userID),
		},
	}
}

func GetExternalPrincipalPermissions(principalId string) Node {
	return Node{
		Permission: Permission{
			Action:   ReadExternalPrincipalAction,
			Resource: ExternalPrincipalArn(principalId),
		},
	}
}

func ListUserExternalPrincipalsPermissions(userID string) Node {
	return Node{
		Permission: Permission{
			Action:   ReadUserAction,
			Resource: UserArn(userID),
		},
	}
}

func GetStorageConfigPermissions() Node {
	return Node{
		Permission: Permission{
			Action:   ReadConfigAction,
			Resource: All,
		},
	}
}

func GetConfigPermissions() Node {
	return Node{
		Permission: Permission{
			Action:   ReadConfigAction,
			Resource: All,
		},
	}
}
