package export

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/parade"
)

// TODO(ariels): replace catalog.Differences with an iterator.

var (
	ErrMissingColumns = errors.New("missing columns in differences result")
	ErrConflict       = errors.New("cannot generate task for conflict in diff")
)

const successFilename = "_lakefs_success"

const (
	StartAction  = "export:start"
	CopyAction   = "export:copy"
	DeleteAction = "export:delete"
	TouchAction  = "export:touch"
	DoneAction   = "export:done"
)

type StartData struct {
	Repo          string `json:"repo"`
	Branch        string `json:"branch"`
	FromCommitRef string `json:"from"`
	ToCommitRef   string `json:"to"`
	ExportID      string `json:"export_id"`
	ExportConfig  catalog.ExportConfiguration
}

type CopyData struct {
	From string `json:"from"`
	To   string `json:"to"`
	ETag string `json:"etag"` // Empty for now :-(
}

type DeleteData struct {
	File string `json:"file"`
}

type SuccessData struct {
	File string `json:"file"`
}

type FinishData struct {
	Repo       string `json:"repo"`
	Branch     string `json:"branch"`
	CommitRef  string `json:"commitRef"`
	StatusPath string `json:"status_path"`
}

// Returns the "dirname" of path: everything up to the last "/" (excluding that slash).  If
// there are no slashes, returns an empty string.
func dirname(path string) string {
	i := strings.LastIndex(path, "/")
	if i == -1 {
		return ""
	}
	return path[0:i]
}

type DirMatchCache struct {
	pred         func(path string) bool
	upMatchCache map[string]*string
}

func (dmc *DirMatchCache) Lookup(filename string) (string, bool) {
	dir := filename
	var ret *string
	for {
		dir = dirname(dir)
		var ok bool
		if ret, ok = dmc.upMatchCache[dir]; ok {
			break
		}
		if dmc.pred(dir) {
			copy := dir
			ret = &copy
			break
		}
		if dir == "" {
			break
		}
	}
	for dir = dirname(filename); dir != ""; dir = dirname(dir) {
		dmc.upMatchCache[dir] = ret
		if ret != nil && dir == *ret {
			break
		}
	}
	if dir == "" {
		// Cache empty result at top of tree
		dmc.upMatchCache[""] = ret
	}

	if ret == nil {
		return "", false
	}
	return *ret, true
}

func NewDirMatchCache(pred func(path string) bool) *DirMatchCache {
	return &DirMatchCache{pred: pred, upMatchCache: make(map[string]*string)}
}

// taskIdGenerator generates IDs for export tasks based on its exportID.
type TaskIDGenerator string

func (exportID TaskIDGenerator) makeSuccessTaskID(path string) parade.TaskID {
	return parade.TaskID(fmt.Sprintf("%s:make-success:%s", exportID, path))
}

func (exportID TaskIDGenerator) finishedTaskID() parade.TaskID {
	return parade.TaskID(fmt.Sprintf("%s:finish", exportID))
}

func (exportID TaskIDGenerator) startedTaskID() parade.TaskID {
	return parade.TaskID(fmt.Sprintf("%s:start", exportID))
}
func (exportID TaskIDGenerator) CopyTaskID(path string) parade.TaskID {
	shaHash := sha256.Sum256([]byte(path))
	shaHashInString := hex.EncodeToString(shaHash[:])
	return parade.TaskID(fmt.Sprintf("%s:copy:%s", exportID, shaHashInString))
}

func (exportID TaskIDGenerator) DeleteTaskID(path string) parade.TaskID {
	shaHash := sha256.Sum256([]byte(path))
	shaHashInString := hex.EncodeToString(shaHash[:])
	return parade.TaskID(fmt.Sprintf("%s:delete:%s", exportID, shaHashInString))
}

// SuccessTasksTreeGenerator accumulates success tasks during task generator.  It is exported
// (only) for testing.
type SuccessTasksTreeGenerator struct {
	idGen                   TaskIDGenerator
	successDirectoriesCache *DirMatchCache
	makeDestination         func(string) string
	finishedTask            parade.TaskData
	successTaskForDirectory map[string]parade.TaskData
}

func NewSuccessTasksTreeGenerator(exportID string, generateSuccessFor func(path string) bool, makeDestination func(string) string, finishBody *string) SuccessTasksTreeGenerator {
	idGen := TaskIDGenerator(exportID)
	zero, one := 0, 1
	return SuccessTasksTreeGenerator{
		idGen:                   idGen,
		successDirectoriesCache: NewDirMatchCache(generateSuccessFor),
		makeDestination:         makeDestination,
		finishedTask: parade.TaskData{
			ID:                idGen.finishedTaskID(),
			Action:            DoneAction,
			Body:              finishBody,
			StatusCode:        parade.TaskPending,
			MaxTries:          &one,
			TotalDependencies: &zero,
		},
		successTaskForDirectory: make(map[string]parade.TaskData),
	}
}

// AddFor adds a dependency task for path (there will always be exactly one, either to create a
// success file after path, or to finish everything), and returns its task ID for the caller to
// signal when done.
func (s *SuccessTasksTreeGenerator) AddFor(path string) (parade.TaskID, error) {
	numTouchTries := 5
	if d, ok := s.successDirectoriesCache.Lookup(path); ok {
		task, ok := s.successTaskForDirectory[d]
		if !ok {
			// Initialize a new task
			task.ID = s.idGen.makeSuccessTaskID(d)
			task.Action = TouchAction
			data := SuccessData{File: s.makeDestination(d + "/" + successFilename)}
			body, err := json.Marshal(data)
			if err != nil {
				return "", fmt.Errorf("failed to serialize %+v: %w", data, err)
			}
			bodyStr := string(body)
			task.Body = &bodyStr
			task.StatusCode = parade.TaskPending
			task.MaxTries = &numTouchTries

			// Success task also has a dependency
			parentID, err := s.AddFor(d)
			if err != nil {
				return "", fmt.Errorf("create parent for %s: %w", d, err)
			}
			task.ToSignalAfter = []parade.TaskID{parentID}
			count := 0
			task.TotalDependencies = &count
		}
		(*task.TotalDependencies)++
		s.successTaskForDirectory[d] = task
		return task.ID, nil
	}
	(*s.finishedTask.TotalDependencies)++
	return s.finishedTask.ID, nil
}

// GenerateTasksTo generates and appends all success tasks and the finished task to tasks,
// returning a new tasks slice.
func (s *SuccessTasksTreeGenerator) GenerateTasksTo(tasks []parade.TaskData) []parade.TaskData {
	l := len(tasks)
	ret := make([]parade.TaskData, l, l+1+len(s.successTaskForDirectory))
	copy(ret, tasks)
	tasks = ret
	for _, task := range s.successTaskForDirectory {
		tasks = append(tasks, task)
	}
	tasks = append(tasks, s.finishedTask)
	return tasks
}

// makeDiffTaskBody fills TaskData *out with id, action and a body to make it a task to
// perform diff.
func makeDiffTaskBody(out *parade.TaskData, idGen TaskIDGenerator, diff catalog.Difference, makeDestination func(string) string) error {
	var data interface{}
	switch diff.Type {
	case catalog.DifferenceTypeAdded, catalog.DifferenceTypeChanged:
		data = CopyData{
			From: diff.PhysicalAddress,
			To:   makeDestination(diff.Path),
		}
		out.ID = idGen.CopyTaskID(diff.Path)
		out.Action = CopyAction
	case catalog.DifferenceTypeRemoved:
		data = DeleteData{
			File: makeDestination(diff.Path),
		}
		out.ID = idGen.DeleteTaskID(diff.Path)
		out.Action = DeleteAction
	case catalog.DifferenceTypeConflict:
		return fmt.Errorf("%+v: %w", diff, ErrConflict)
	}
	body, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("%+v: failed to serialize %+v: %w", diff, data, err)
	}
	bodyStr := string(body)
	out.Body = &bodyStr
	return nil
}

// TasksGenerator generates tasks from diffs iteratively.
type TasksGenerator struct {
	ExportID           string
	DstPrefix          string
	GenerateSuccessFor func(path string) bool
	NumTries           int

	makeDestination       func(string) string
	idGen                 TaskIDGenerator
	successTasksGenerator SuccessTasksTreeGenerator
}

func GetStartTasks(repo, branch, fromCommitRef, toCommitRef, exportID string, config catalog.ExportConfiguration) ([]parade.TaskData, error) {
	one, zero := 1, 0
	data := StartData{
		Repo:          repo,
		Branch:        branch,
		FromCommitRef: fromCommitRef,
		ToCommitRef:   toCommitRef,
		ExportID:      exportID,
		ExportConfig:  config,
	}
	body, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize %+v: %w", data, err)
	}

	bodyStr := string(body)
	idGen := TaskIDGenerator(exportID)
	tasks := make([]parade.TaskData, 1)
	tasks[0] = parade.TaskData{
		ID:                idGen.startedTaskID(),
		Action:            StartAction,
		Body:              &bodyStr,
		StatusCode:        parade.TaskPending,
		MaxTries:          &one,
		TotalDependencies: &zero,
	}
	return tasks, nil
}

// NewTasksGenerator returns a generator that exports tasks from diffs to file operations under
// dstPrefix.  It generates success files for files in directories matched by
// "generateSuccessFor".
func NewTasksGenerator(exportID string, dstPrefix string, generateSuccessFor func(path string) bool, finishBody *string) *TasksGenerator {
	const numTries = 5
	dstPrefix = strings.TrimRight(dstPrefix, "/")
	makeDestination := func(path string) string {
		return fmt.Sprintf("%s/%s", dstPrefix, path)
	}

	return &TasksGenerator{
		ExportID:              exportID,
		DstPrefix:             dstPrefix,
		GenerateSuccessFor:    generateSuccessFor,
		NumTries:              numTries,
		makeDestination:       makeDestination,
		idGen:                 TaskIDGenerator(exportID),
		successTasksGenerator: NewSuccessTasksTreeGenerator(exportID, generateSuccessFor, makeDestination, finishBody),
	}
}

// Add translates diffs into many tasks and remembers "generate success" tasks for Finish.  It
// returns some tasks that can already be added.
func (e *TasksGenerator) Add(diffs catalog.Differences) ([]parade.TaskData, error) {
	const initialSize = 1_000

	zero := 0

	ret := make([]parade.TaskData, 0, initialSize)

	// Create file operation tasks to return
	for _, diff := range diffs {
		if diff.Path == "" {
			return nil, fmt.Errorf("no \"Path\" in %+v: %w", diff, ErrMissingColumns)
		}

		task := parade.TaskData{
			StatusCode:        parade.TaskPending,
			MaxTries:          &e.NumTries,
			TotalDependencies: &zero, // Depends only on a start task
		}
		err := makeDiffTaskBody(&task, e.idGen, diff, e.makeDestination)
		if err != nil {
			return ret, err
		}
		id, err := e.successTasksGenerator.AddFor(diff.Path)
		if err != nil {
			return ret, fmt.Errorf("generate tasks after %+v: %w", diff, err)
		}
		task.ToSignalAfter = []parade.TaskID{id}

		ret = append(ret, task)
	}

	return ret, nil
}

// Finish ends tasks generation, releasing any tasks for success and finish.
func (e *TasksGenerator) Finish() ([]parade.TaskData, error) {
	ret := make([]parade.TaskData, 0)
	ret = e.successTasksGenerator.GenerateTasksTo(ret)

	return ret, nil
}
