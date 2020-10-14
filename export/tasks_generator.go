package export

import (
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
	CopyAction   = "export:copy"
	DeleteAction = "export:delete"
	TouchAction  = "export:touch"
	DoneAction   = "export:done"
)

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
type taskIDGenerator string

func (exportID taskIDGenerator) makeSuccessTaskID(path string) parade.TaskID {
	return parade.TaskID(fmt.Sprintf("%s:make-success:%s", exportID, path))
}

func (exportID taskIDGenerator) finishedTaskID() parade.TaskID {
	return parade.TaskID(fmt.Sprintf("%s:finished", exportID))
}

func (exportID taskIDGenerator) copyTaskID(physicalAddress string) parade.TaskID {
	return parade.TaskID(fmt.Sprintf("%s:copy:%s", exportID, physicalAddress))
}

func (exportID taskIDGenerator) deleteTaskID(physicalAddress string) parade.TaskID {
	return parade.TaskID(fmt.Sprintf("%s:delete:%s", exportID, physicalAddress))
}

// SuccessTasksTreeGenerator accumulates success tasks during task generator.  It is exported
// (only) for testing.
type SuccessTasksTreeGenerator struct {
	idGen                   taskIDGenerator
	successDirectoriesCache *DirMatchCache
	makeDestination         func(string) string
	finishedTask            parade.TaskData
	successTaskForDirectory map[string]parade.TaskData
}

func NewSuccessTasksTreeGenerator(exportID string, generateSuccessFor func(path string) bool, makeDestination func(string) string) SuccessTasksTreeGenerator {
	idGen := taskIDGenerator(exportID)
	zero, one := 0, 1
	return SuccessTasksTreeGenerator{
		idGen:                   idGen,
		successDirectoriesCache: NewDirMatchCache(generateSuccessFor),
		makeDestination:         makeDestination,
		finishedTask: parade.TaskData{
			ID:                idGen.finishedTaskID(),
			Action:            DoneAction,
			Body:              nil,
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
			data := SuccessData{File: s.makeDestination(d)}
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
	off := len(tasks)
	tasks = tasks[:off+1+len(s.successTaskForDirectory)]
	for _, task := range s.successTaskForDirectory {
		tasks[off] = task
		off++
	}
	tasks[off] = s.finishedTask
	return tasks
}

// makeDiffTaskBody fills TaskData *out with id, action and a body to make it a task to
// perform diff.
func makeDiffTaskBody(out *parade.TaskData, idGen taskIDGenerator, diff catalog.Difference, makeDestination func(string) string) error {
	var (
		data interface{}
		err  error
	)
	switch diff.Type {
	case catalog.DifferenceTypeAdded, catalog.DifferenceTypeChanged:
		data = CopyData{
			From: diff.PhysicalAddress,
			To:   makeDestination(diff.Path),
		}
		out.ID = idGen.copyTaskID(diff.PhysicalAddress)
		out.Action = CopyAction
	case catalog.DifferenceTypeRemoved:
		data = DeleteData{
			File: makeDestination(diff.Path),
		}
		out.ID = idGen.deleteTaskID(diff.PhysicalAddress)
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

// GenerateTasksFromDiffs converts diffs into many tasks that depend on startTaskID, with a
// "generate success" task after generating all files in each directory that matches
// generateSuccessFor.
func GenerateTasksFromDiffs(exportID string, dstPrefix string, diffs catalog.Differences, generateSuccessFor func(path string) bool) ([]parade.TaskData, error) {
	const initialSize = 1_000

	one := 1 // Number of dependencies of many tasks.  This will *not* change.
	numTries := 5

	dstPrefix = strings.TrimRight(dstPrefix, "/")
	makeDestination := func(path string) string {
		return fmt.Sprintf("%s/%s", dstPrefix, path)
	}

	idGen := taskIDGenerator(exportID)

	successTasksGenerator := NewSuccessTasksTreeGenerator(
		exportID, generateSuccessFor, makeDestination)

	ret := make([]parade.TaskData, 0, initialSize)

	// Create the file operation tasks
	for _, diff := range diffs {
		if diff.Path == "" {
			return nil, fmt.Errorf("no \"Path\" in %+v: %w", diff, ErrMissingColumns)
		}

		task := parade.TaskData{
			StatusCode:        parade.TaskPending,
			MaxTries:          &numTries,
			TotalDependencies: &one, // Depends only on a start task
		}
		err := makeDiffTaskBody(&task, idGen, diff, makeDestination)
		if err != nil {
			return ret, err
		}
		id, err := successTasksGenerator.AddFor(diff.Path)
		if err != nil {
			return ret, fmt.Errorf("generate tasks after %+v: %w", diff, err)
		}
		task.ToSignalAfter = []parade.TaskID{id}

		ret = append(ret, task)
	}

	ret = successTasksGenerator.GenerateTasksTo(ret)

	return ret, nil
}
