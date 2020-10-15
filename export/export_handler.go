package export

import (
	"encoding/json"
	"fmt"
	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/logging"
	"github.com/treeverse/lakefs/parade"
	"net/url"
	"strings"
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
	u, err := url.Parse(path) //TODO: add verify path on create task
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
	return h.adapter.Copy(from, to) // todo(guys): add wait for copy in handler
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
func (h *Handler) Handle(action string, body *string) parade.HandlerResult {

	var err error
	switch action {
	case CopyAction:
		err = h.copy(body)
	case DeleteAction:
		err = h.remove(body)
	case TouchAction:
		err = h.touch(body)
	case DoneAction:
		//todo(guys): handle done action
	default:
		err = fmt.Errorf("unknown action")
	}

	if err != nil {
		logging.Default().WithFields(logging.Fields{
			"actor":  actorName,
			"action": action,
		}).WithError(err).Error("touch failed")

		return parade.HandlerResult{
			Status:     err.Error(),
			StatusCode: parade.TaskInvalid,
		}
	}
	return parade.HandlerResult{
		Status:     fmt.Sprintf("Completed"),
		StatusCode: parade.TaskCompleted,
	}
}

func (h *Handler) Actions() []string {
	return []string{CopyAction, DeleteAction, TouchAction, DoneAction}
}

func (h *Handler) Actor() parade.ActorID {
	return actorName
}
