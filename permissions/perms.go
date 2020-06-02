package permissions

type Permission string

const (
	ReadRepo    Permission = "repos:Read"
	WriteRepo   Permission = "repos:Write"
	ManageRepos Permission = "repos:Manage"
	ManageRBAC  Permission = "rbac:Manage"
)
