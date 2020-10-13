package action

import "github.com/treeverse/lakefs/parade"

type HandlerResult struct {
	Status     string
	StatusCode parade.TaskStatusCodeValue
}

type TaskHandler interface {
	Handle(action string, body *string) HandlerResult
	Actions() []string
	Actor() parade.ActorID
}
