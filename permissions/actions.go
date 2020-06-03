package permissions

type Action string

const (
	ReadRepo    Action = "repos:Read"
	WriteRepo   Action = "repos:Write"
	ManageRepos Action = "repos:Manage"
	ManageRBAC  Action = "rbac:Manage"
)

var actionSet = map[Action]struct{}{
	ReadRepo:    struct{}{},
	WriteRepo:   struct{}{},
	ManageRepos: struct{}{},
	ManageRBAC:  struct{}{},
}

func IsAction(action string) bool {
	_, ok := actionSet[Action(action)]
	return ok
}
