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

func TemplateArn(templateID string) string {
	return fsArnPrefix + "template/" + templateID
}
