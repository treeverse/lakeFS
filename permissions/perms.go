package permissions

type Permission string

const (
	ReadRepo    Permission = "repos:Read"
	WriteRepo   Permission = "repos:Write"
	ManageRepos Permission = "repos:manage"
)
