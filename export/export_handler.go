package export

import (
	"encoding/json"
	"fmt"
	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/logging"
	"github.com/treeverse/lakefs/parade"
	"strings"
)

const actorName parade.ActorID = "EXPORT"
const (
	actionCopy   = "export-copy"
	actionDelete = "export-delete"
	actionTouch  = "export-touch"
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

func (h *Handler) Handle(action string, body *string) parade.HandlerResult {
	var params TaskBody
	lg := logging.Default().WithFields(logging.Fields{
		"actor":  actorName,
		"action": action,
	})
	err := json.Unmarshal([]byte(*body), &params)
	if err != nil {
		lg.WithError(err).Error("unmarshal failed")
		return parade.HandlerResult{
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
			lg.WithError(err).Error("copy failed")
			return parade.HandlerResult{
				Status:     err.Error(),
				StatusCode: parade.TaskInvalid,
			}
		}
	case actionDelete:
		err = h.adapter.Remove(destinationPointer)
		if err != nil {
			lg.WithError(err).Error("delete failed")
			return parade.HandlerResult{
				Status:     err.Error(),
				StatusCode: parade.TaskInvalid,
			}
		}
	case actionTouch:
		err = h.adapter.Put(destinationPointer, 0, strings.NewReader(""), block.PutOpts{})
		if err != nil {
			lg.WithError(err).Error("touch failed")
			return parade.HandlerResult{
				Status:     err.Error(),
				StatusCode: parade.TaskInvalid,
			}
		}
	//todo(guys): add cases for other actions or remove them from Actions function
	default:
		lg.Error("unknown action")
		return parade.HandlerResult{
			Status:     "UNKNOWN ACTION",
			StatusCode: parade.TaskInvalid,
		}
	}
	return parade.HandlerResult{
		Status:     fmt.Sprintf("Completed"),
		StatusCode: parade.TaskCompleted,
	}
}

func (h *Handler) Actions() []string {
	return []string{actionCopy, actionDelete, actionNext, actionStart, actionDone}
}

func (h *Handler) Actor() parade.ActorID {
	return actorName
}
