package permissions

import (
	"fmt"
	"strings"
)

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

func placeholder(key string) string {
	return fmt.Sprintf("{%s}", key)
}

func RenamePlaceholder(template, oldKey, newKey string) string {
	oldPlaceholder := fmt.Sprintf("{%s}", oldKey)
	newPlaceholder := fmt.Sprintf("{%s}", newKey)
	return strings.ReplaceAll(template, oldPlaceholder, newPlaceholder)
}

func FillTemplate(template string, values map[string]string) string {
	for key, value := range values {
		template = strings.ReplaceAll(template, placeholder(key), value)
	}
	return template
}

const (
	RepoID    = "repoID"
	BranchID  = "branchID"
	ObjectKey = "key"
	SrcKey    = "srcKey"
	DestKey   = "destKey"
)

var (
	RepoArnTemplate       = fsArnPrefix + "repository/" + placeholder(RepoID)
	BranchArnTemplate     = fsArnPrefix + "repository/" + placeholder(RepoID) + "/branch/" + placeholder(BranchID)
	ObjectArnTemplate     = fsArnPrefix + "repository/" + placeholder(RepoID) + "/object/" + placeholder(ObjectKey)
	SrcObjectArnTemplate  = RenamePlaceholder(ObjectArnTemplate, ObjectKey, SrcKey)
	DestObjectArnTemplate = RenamePlaceholder(ObjectArnTemplate, ObjectKey, DestKey)
)

func FillPermissionTemplate(node *Node, values map[string]string) {
	node.Permission.Resource = FillTemplate(node.Permission.Resource, values)

	for i := range node.Nodes {
		FillPermissionTemplate(&node.Nodes[i], values)
	}
}

type PermissionFactory func() Node

var PermissionFactories = map[string]PermissionFactory{
	"copy_object": CopyObjectPermissions,
}

func CreatePermission(action string, values map[string]string) Node {
	permissionNode := PermissionFactories[action]()
	FillPermissionTemplate(&permissionNode, values)
	return permissionNode
}

func CopyObjectPermissions() Node {
	return Node{
		Type: NodeTypeAnd,
		Nodes: []Node{
			{
				Permission: Permission{
					Action:   ReadObjectAction,
					Resource: SrcObjectArnTemplate,
				},
			},
			{
				Permission: Permission{
					Action:   WriteObjectAction,
					Resource: DestObjectArnTemplate,
				},
			},
		},
	}
}
