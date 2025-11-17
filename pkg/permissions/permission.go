package permissions

const (
	fsArnPrefix   = "arn:lakefs:fs:::"
	authArnPrefix = "arn:lakefs:auth:::"

	All = "*"
)

type Permission struct {
	Action   string
	Resource string
}

type NodeType int

const (
	NodeTypeNode NodeType = iota
	NodeTypeOr
	NodeTypeAnd
)

type Node struct {
	Type       NodeType
	Permission Permission
	Nodes      []Node
}

func RepoArn(repoID string) string {
	return fsArnPrefix + "repository/" + repoID
}

func StorageNamespace(namespace string) string {
	return fsArnPrefix + "namespace/" + namespace
}

func ObjectArn(repoID, key string) string {
	return fsArnPrefix + "repository/" + repoID + "/object/" + key
}

func BranchArn(repoID, branchID string) string {
	return fsArnPrefix + "repository/" + repoID + "/branch/" + branchID
}

func TagArn(repoID, tagID string) string {
	return fsArnPrefix + "repository/" + repoID + "/tag/" + tagID
}

func UserArn(userID string) string {
	return authArnPrefix + "user/" + userID
}

func GroupArn(groupID string) string {
	return authArnPrefix + "group/" + groupID
}

func PolicyArn(policyID string) string {
	return authArnPrefix + "policy/" + policyID
}

func ExternalPrincipalArn(principalID string) string {
	return authArnPrefix + "externalPrincipal/" + principalID
}

type PermissionParams struct {
	Repository *string
	Path       *string
	Branch     *string
}

type PermissionDescriptor interface {
	Permission(PermissionParams) Node
}

type ObjectPermission struct {
	Action string
}

func (o *ObjectPermission) Permission(params PermissionParams) Node {
	return Node{
		Permission: Permission{
			Action:   o.Action,
			Resource: ObjectArn(*params.Repository, *params.Path),
		},
	}
}

type BranchPermission struct {
	Action string
}

func (o *BranchPermission) Permission(params PermissionParams) Node {
	return Node{
		Permission: Permission{
			Action:   o.Action,
			Resource: ObjectArn(*params.Repository, *params.Branch),
		},
	}
}

type RepoPermission struct {
	Action string
}

func (r *RepoPermission) Permission(params PermissionParams) Node {
	return Node{
		Permission: Permission{
			Action:   r.Action,
			Resource: RepoArn(*params.Repository),
		},
	}
}

var readObjectPermission = ObjectPermission{Action: ReadObjectAction}
var writeObjectPermission = ObjectPermission{Action: WriteObjectAction}
var createBranchPermission = BranchPermission{Action: CreateBranchAction}
var deleteBranchPermission = BranchPermission{Action: DeleteBranchAction}
var readBranchPermission = BranchPermission{Action: ReadBranchAction}
var revertBranchPermission = BranchPermission{Action: RevertBranchAction}
var createCommitPermission = BranchPermission{Action: CreateCommitAction}
var importCancelPermission = BranchPermission{Action: ImportCancelAction}
var prepareGCUncommittedPermission = RepoPermission{Action: PrepareGarbageCollectionUncommittedAction}
var getGCRulesPermission = RepoPermission{Action: GetGarbageCollectionRulesAction}
var setGCRulesPermission = RepoPermission{Action: SetGarbageCollectionRulesAction}
var prepareGCCommitsPermission = RepoPermission{Action: PrepareGarbageCollectionCommitsAction}
var readRepositoryPermission = RepoPermission{Action: ReadRepositoryAction}
var updateRepositoryPermission = RepoPermission{Action: UpdateRepositoryAction}
var deleteRepositoryPermission = RepoPermission{Action: DeleteRepositoryAction}
var writePullRequestPermission = RepoPermission{Action: WritePullRequestAction}
var listPullRequestsPermission = RepoPermission{Action: ListPullRequestsAction}
var readPullRequestPermission = RepoPermission{Action: ReadPullRequestAction}
var listBranchesPermission = RepoPermission{Action: ListBranchesAction}
var listTagsPermission = RepoPermission{Action: ListTagsAction}
var listObjectsPermission = RepoPermission{Action: ListObjectsAction}

var permissionByOp = map[string]PermissionDescriptor{
	"HeadObject":                          &readObjectPermission,
	"GetObject":                           &readObjectPermission,
	"StatObject":                          &readObjectPermission,
	"GetUnderlyingProperties":             &readObjectPermission,
	"StageObject":                         &writeObjectPermission,
	"CreateSymlinkFile":                   &writeObjectPermission,
	"UpdateObjectUserMetadata":            &writeObjectPermission,
	"UploadObject":                        &writeObjectPermission,
	"UploadObjectPreflight":               &writeObjectPermission,
	"CreateBranch":                        &createBranchPermission,
	"DeleteBranch":                        &deleteBranchPermission,
	"GetBranch":                           &readBranchPermission,
	"RevertBranch":                        &revertBranchPermission,
	"LogCommits":                          &readBranchPermission,
	"ResetBranch":                         &revertBranchPermission,
	"MergeIntoBranch":                     &createCommitPermission,
	"HardResetBranch":                     &revertBranchPermission,
	"ImportStatus":                        &readBranchPermission,
	"Commit":                              &createCommitPermission,
	"ImportCancel":                        &importCancelPermission,
	"PrepareGarbageCollectionUncommitted": &prepareGCUncommittedPermission,
	"GetGCRules":                          &getGCRulesPermission,
	"SetGCRules":                          &setGCRulesPermission,
	"DeleteGCRules":                       &setGCRulesPermission,
	"SetGarbageCollectionRulesPreflight":  &setGCRulesPermission,
	"PrepareGarbageCollectionCommits":     &prepareGCCommitsPermission,
	"GetRepository":                       &readRepositoryPermission,
	"GetRepositoryMetadata":               &readRepositoryPermission,
	"SetRepositoryMetadata":               &updateRepositoryPermission,
	"DeleteRepositoryMetadata":            &updateRepositoryPermission,
	"DeleteRepository":                    &deleteRepositoryPermission,
	"UpdatePullRequest":                   &writePullRequestPermission,
	"CreatePullRequest":                   &writePullRequestPermission,
	"ListPullRequests":                    &listPullRequestsPermission,
	"GetPullRequest":                      &readPullRequestPermission,
	"ListBranches":                        &listBranchesPermission,
	"ListTags":                            &listTagsPermission,
	"ListObjects":                         &listObjectsPermission,
}

func GetPermissionDescriptor(operationId string) PermissionDescriptor {
	return permissionByOp[operationId]
}
