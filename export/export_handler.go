package export

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/logging"
	"github.com/treeverse/lakefs/parade"
)

const actorName parade.ActorID = "EXPORT"

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

func PathToPointer(path string) (block.ObjectPointer, error) {
	u, err := url.Parse(path) // TODO(guys): add verify path on create task
	if err != nil {
		return block.ObjectPointer{}, err
	}
	return block.ObjectPointer{
		StorageNamespace: fmt.Sprintf("%s://%s", u.Scheme, u.Host),
		Identifier:       u.Path,
	}, err
}
func (h *Handler) copy(body *string) error {
	var copyData CopyData
	err := json.Unmarshal([]byte(*body), &copyData)
	if err != nil {
		return err
	}
	from, err := PathToPointer(copyData.From)
	if err != nil {
		return err
	}
	to, err := PathToPointer(copyData.To)
	if err != nil {
		return err
	}
	return h.adapter.Copy(from, to) // TODO(guys): add wait for copy in handler
}

func (h *Handler) remove(body *string) error {
	var deleteData DeleteData
	err := json.Unmarshal([]byte(*body), &deleteData)
	if err != nil {
		return err
	}
	path, err := PathToPointer(deleteData.File)
	if err != nil {
		return err
	}
	return h.adapter.Remove(path)
}

func (h *Handler) touch(body *string) error {
	var successData SuccessData
	err := json.Unmarshal([]byte(*body), &successData)
	if err != nil {
		return err
	}
	path, err := PathToPointer(successData.File)
	if err != nil {
		return err
	}
	return h.adapter.Put(path, 0, strings.NewReader(""), block.PutOpts{})
}

var errUnknownAction = errors.New("unknown action")

func (h *Handler) Handle(action string, body *string) parade.ActorResult {
	var err error
	switch action {
	case CopyAction:
		err = h.copy(body)
	case DeleteAction:
		err = h.remove(body)
	case TouchAction:
		err = h.touch(body)
	case DoneAction:
		// TODO(guys): handle done action
	default:
		err = errUnknownAction
	}

	if err != nil {
		logging.Default().WithFields(logging.Fields{
			"actor":  actorName,
			"action": action,
		}).WithError(err).Error("touch failed")

		return parade.ActorResult{
			Status:     err.Error(),
			StatusCode: parade.TaskInvalid,
		}
	}
	return parade.ActorResult{
		Status:     "Completed",
		StatusCode: parade.TaskCompleted,
	}
}

func (h *Handler) Actions() []string {
	return []string{CopyAction, DeleteAction, TouchAction, DoneAction}
}

func (h *Handler) ActorID() parade.ActorID {
	return actorName
}
