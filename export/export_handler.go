package export

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"

	"github.com/treeverse/lakefs/catalog"

	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/logging"
	"github.com/treeverse/lakefs/parade"
)

const actorName parade.ActorID = "EXPORT"

type Handler struct {
	adapter   block.Adapter
	cataloger catalog.Cataloger
	parade    parade.Parade
}

func NewHandler(adapter block.Adapter, cataloger catalog.Cataloger, parade parade.Parade) *Handler {
	return &Handler{
		adapter:   adapter,
		cataloger: cataloger,
		parade:    parade,
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

func (h *Handler) start(body *string) error {
	var startData StartData
	err := json.Unmarshal([]byte(*body), &startData)
	if err != nil {
		return err
	}

	finishBodyStr, err := getFinishBodyString(startData.Repo, startData.Branch, startData.ToCommitRef, startData.ExportConfig.StatusPath)
	if err != nil {
		return err
	}
	return h.generateTasks(startData, startData.ExportConfig, &finishBodyStr)
}

func (h *Handler) generateTasks(startData StartData, config catalog.ExportConfiguration, finishBodyStr *string) error {
	tasksGenerator := NewTasksGenerator(startData.ExportID, config.Path, getGenerateSuccess(config.LastKeysInPrefixRegexp), finishBodyStr)
	var diffs catalog.Differences
	var err error
	var hasMore bool
	after := ""
	limit := -1
	diffFromBase := startData.FromCommitRef == ""
	for {
		if diffFromBase {
			diffs, hasMore, err = getDiffFromBase(context.Background(), startData.Repo, startData.ToCommitRef, after, limit, h.cataloger)
		} else {
			// Todo(guys) change this to work with diff iterator once it is available outside of cataloger
			diffs, hasMore, err = h.cataloger.Diff(context.Background(), startData.Repo, startData.ToCommitRef, startData.FromCommitRef, catalog.DiffParams{
				Limit:            limit,
				After:            after,
				AdditionalFields: []string{"physical_address"},
			})
		}
		if err != nil {
			return err
		}
		if len(diffs) == 0 {
			break
		}
		taskData, err := tasksGenerator.Add(diffs)
		if err != nil {
			return err
		}
		// add taskData tasks
		err = h.parade.InsertTasks(context.Background(), taskData)
		if err != nil {
			return err
		}

		if !hasMore {
			break
		}
		after = diffs[len(diffs)-1].Path
	}

	taskData, err := tasksGenerator.Finish()
	if err != nil {
		return err
	}
	return h.parade.InsertTasks(context.Background(), taskData)
	// Todo(guys): add generate success for, and success path
}

// getDiffFromBase returns all the entries on the ref as diffs
func getDiffFromBase(ctx context.Context, repo, ref, after string, limit int, cataloger catalog.Cataloger) (catalog.Differences, bool, error) {
	entries, hasMore, err := cataloger.ListEntries(ctx, repo, ref, "", after, "", limit)
	if err != nil {
		return nil, false, err
	}
	return entriesToDiff(entries), hasMore, nil
}

func entriesToDiff(entries []*catalog.Entry) []catalog.Difference {
	res := make([]catalog.Difference, len(entries))
	for i, entry := range entries {
		res[i] = catalog.Difference{
			Entry: *entry,
			Type:  catalog.DifferenceTypeAdded,
		}
	}
	return res
}

func getGenerateSuccess(lastKeysInPrefixRegexp []string) func(path string) bool {
	return func(path string) bool {
		for _, regex := range lastKeysInPrefixRegexp {
			match, err := regexp.MatchString(regex, path)
			if err != nil {
				// ignore failing for now
				// Todo(guys) - handle this
				return false
			}
			if match {
				return true
			}
		}
		return false
	}
}

func getFinishBodyString(repo, branch, commitRef, statusPath string) (string, error) {
	finishData := FinishData{
		Repo:       repo,
		Branch:     branch,
		CommitRef:  commitRef,
		StatusPath: statusPath,
	}
	finisBody, err := json.Marshal(finishData)
	if err != nil {
		return "", err
	}
	return string(finisBody), nil
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

func getStatus(signalledErrors int) (catalog.CatalogBranchExportStatus, *string) {
	if signalledErrors > 0 {
		msg := fmt.Sprintf("%d tasks failed\n", signalledErrors)
		return catalog.ExportStatusFailed, &msg
	}
	return catalog.ExportStatusSuccess, nil
}
func (h *Handler) done(body *string, signalledErrors int) error {
	var finishData FinishData
	err := json.Unmarshal([]byte(*body), &finishData)
	if err != nil {
		return err
	}

	status, msg := getStatus(signalledErrors)
	fileName := fmt.Sprintf("%s-%s-%s", finishData.Repo, finishData.Branch, finishData.CommitRef)
	path, err := PathToPointer(fmt.Sprintf("%s/%s", finishData.StatusPath, fileName))
	if err != nil {
		return err
	}
	data := fmt.Sprintf("status: %s, signalled_errors: %d\n", status, signalledErrors)
	reader := strings.NewReader(data)
	err = h.adapter.Put(path, reader.Size(), reader, block.PutOpts{})
	if err != nil {
		return err
	}
	return ExportBranchDone(h.cataloger, status, msg, finishData.Branch, finishData.CommitRef, finishData.Repo)
}

var errUnknownAction = errors.New("unknown action")

func (h *Handler) Handle(action string, body *string, signalledErrors int) parade.ActorResult {
	var err error
	switch action {
	case StartAction:
		err = h.start(body)
	case CopyAction:
		err = h.copy(body)
	case DeleteAction:
		err = h.remove(body)
	case TouchAction:
		err = h.touch(body)
	case DoneAction:
		err = h.done(body, signalledErrors)
	default:
		err = errUnknownAction
	}

	if err != nil {
		logging.Default().WithFields(logging.Fields{
			"actor":  actorName,
			"action": action,
		}).WithError(err).Errorf("%s failed", action)

		return parade.ActorResult{
			Status:     err.Error(),
			StatusCode: parade.TaskAborted,
		}
	}
	return parade.ActorResult{
		Status:     "Completed",
		StatusCode: parade.TaskCompleted,
	}
}

func (h *Handler) Actions() []string {
	return []string{StartAction, CopyAction, DeleteAction, TouchAction, DoneAction}
}

func (h *Handler) ActorID() parade.ActorID {
	return actorName
}
