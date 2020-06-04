package permissions

type Action string

const (
	ReadRepoAction    Action = "repos:Read"
	WriteRepoAction   Action = "repos:Write"
	ManageReposAction Action = "repos:Manage"
	ReadAuthAction    Action = "auth:Read"
	WriteAuthAction   Action = "auth:Write"
)

var actionSet = map[Action]struct{}{
	ReadRepoAction:    {},
	WriteRepoAction:   {},
	ManageReposAction: {},
	ReadAuthAction:    {},
	WriteAuthAction:   {},
}

func IsAction(action string) bool {
	_, ok := actionSet[Action(action)]
	return ok
}
