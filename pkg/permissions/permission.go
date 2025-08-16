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

var readObjectPermission = ObjectPermission{Action: ReadObjectAction}
var writeObjectPermission = ObjectPermission{Action: WriteObjectAction}

var permissionByOp = map[string]PermissionDescriptor{
	"HeadObject":               &readObjectPermission,
	"GetObject":                &readObjectPermission,
	"StatObject":               &readObjectPermission,
	"GetUnderlyingProperties":  &readObjectPermission,
	"StageObject":              &writeObjectPermission,
	"CreateSymlinkFile":        &writeObjectPermission,
	"UpdateObjectUserMetadata": &writeObjectPermission,
	"UploadObject":             &writeObjectPermission,
	"UploadObjectPreflight":    &writeObjectPermission,
}

func GetPermissionDescriptor(operationId string) PermissionDescriptor {
	return permissionByOp[operationId]
}
