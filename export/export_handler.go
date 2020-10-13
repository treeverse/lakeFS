package export

import (
	"encoding/json"
	"fmt"
	act "github.com/treeverse/lakefs/action"
	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/parade"
)

const actorName parade.ActorID = "EXPORT"
const (
	actionCopy   = "export-copy"
	actionDelete = "export-delete"
	actionNext   = "next-export"
	actionStart  = "start-export"
	actionDone   = "done-export"
)

type Handler struct {
	adapter block.Adapter
}

func NewHandler(adapter block.Adapter) *Handler {
	return &Handler{
		adapter: adapter,
	}
}

type TaskBody struct {
	DestinationNamespace string
	DestinationID        string
	SourceNamespace      string
	SourceID             string
}

// todo(guys): add logs
func (h *Handler) Handle(action string, body *string) act.HandlerResult {
	var params TaskBody
	err := json.Unmarshal([]byte(*body), &params)
	if err != nil {
		return act.HandlerResult{
			Status:     err.Error(),
			StatusCode: parade.TaskInvalid,
		}
	}
	destinationPointer := block.ObjectPointer{
		StorageNamespace: params.DestinationNamespace,
		Identifier:       params.DestinationID,
	}
	sourcePointer := block.ObjectPointer{
		StorageNamespace: params.SourceNamespace,
		Identifier:       params.SourceID,
	}

	switch action {
	case actionCopy:
		err = h.adapter.Copy(sourcePointer, destinationPointer) // todo(guys): add wait for copy in handler
		if err != nil {
			return act.HandlerResult{
				Status:     err.Error(),
				StatusCode: parade.TaskInvalid,
			}
		}
	case actionDelete:
		err = h.adapter.Remove(destinationPointer)
		if err != nil {
			return act.HandlerResult{
				Status:     err.Error(),
				StatusCode: parade.TaskInvalid,
			}
		}
	default:
		return act.HandlerResult{
			Status:     "UNKNOWN ACTION",
			StatusCode: parade.TaskInvalid,
		}
	}
	return act.HandlerResult{
		Status:     fmt.Sprintf("Completed"),
		StatusCode: parade.TaskCompleted,
	}
}

func (h *Handler) Actions() []string {
	return []string{actionCopy, actionDelete}
}

func (h *Handler) Actor() parade.ActorID {
	return actorName
}
