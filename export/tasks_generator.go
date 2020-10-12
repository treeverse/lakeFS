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

const CopyActipn = "export:copy"
const DeleteAction = "export:delete"
const TouchAction = "export:touch"
const DoneAction = "export:done"

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

type matchCounter struct {
	pred func(s string) bool
	m    map[string]int
}

// matchAndCount tests s and returns true and increments its counter if it matches.
func (mc *matchCounter) matchAndCount(s string) bool {
	if !mc.pred(s) {
		return false
	}
	mc.m[s]++
	return true
}

// generateTasksFromDiffs converts diffs into many tasks that depend on startTaskID, with a
// "generate success" task after generating all files in each directory that matches
// generateSuccessFor.
func GenerateTasksFromDiffs(exportID string, dstPrefix string, diffs catalog.Differences, generateSuccessFor func(path string) bool) ([]parade.TaskData, error) {
	const initialSize = 100000

	one := 1 // Number of dependencies of many tasks.  This will *not* change.
	numTries := 5

	dstPrefix = strings.TrimRight(dstPrefix, "/")
	makeDestination := func(path string) string {
		return fmt.Sprintf("%s/%s", dstPrefix, path)
	}
	makeSuccessTaskID := func(path string) parade.TaskID {
		return parade.TaskID(fmt.Sprintf("%s:make-success:%s", exportID, path))
	}

	finishedTaskID := parade.TaskID(fmt.Sprintf("%s:finished", exportID))
	finishedTask := parade.TaskData{
		ID:         finishedTaskID,
		Action:     DoneAction,
		Body:       nil,
		StatusCode: parade.TaskPending,
		MaxTries:   &one,
		// TotalDependencies filled in later
	}
	totalTasks := 0

	successForDirectory := matchCounter{pred: generateSuccessFor, m: make(map[string]int)}

	makeTaskForDiff := func(diff *catalog.Difference) (parade.TaskData, error) {
		var (
			body     []byte
			action   string
			taskID   parade.TaskID
			toSignal []parade.TaskID
			err      error
		)

		d := dirname(diff.Path)
		if successForDirectory.matchAndCount(d) {
			toSignal = append(toSignal, makeSuccessTaskID(d))
		}
		toSignal = append(toSignal, finishedTaskID)

		switch diff.Type {
		case catalog.DifferenceTypeAdded:
			fallthrough // Same handling as "change"
		case catalog.DifferenceTypeChanged:
			data := CopyData{
				From: diff.PhysicalAddress,
				To:   makeDestination(diff.Path),
			}
			body, err = json.Marshal(data)
			if err != nil {
				return parade.TaskData{}, fmt.Errorf("%+v: failed to serialize %+v: %w", diff, data, err)
			}
			taskID = parade.TaskID(fmt.Sprintf("%s:copy:%s", exportID, diff.PhysicalAddress))
			action = CopyActipn
		case catalog.DifferenceTypeRemoved:
			data := DeleteData{
				File: makeDestination(diff.Path),
			}
			body, err = json.Marshal(data)
			if err != nil {
				return parade.TaskData{}, fmt.Errorf("%+v: failed to serialize %+v: %w", diff, data, err)
			}
			taskID = parade.TaskID(fmt.Sprintf("%s:delete:%s", exportID, diff.PhysicalAddress))
			action = DeleteAction
		case catalog.DifferenceTypeConflict:
			return parade.TaskData{}, fmt.Errorf("%+v: %w", diff, ErrConflict)
		}
		bodyStr := string(body)
		return parade.TaskData{
			ID:                taskID,
			Action:            action,
			Body:              &bodyStr,
			StatusCode:        parade.TaskPending,
			MaxTries:          &numTries,
			TotalDependencies: &one, // Depends only on a start task
			ToSignalAfter:     toSignal,
		}, nil
	}

	ret := make([]parade.TaskData, 0, initialSize)

	// Create the file operation tasks
	for _, diff := range diffs {
		if diff.Path == "" {
			return nil, fmt.Errorf("no \"Path\" in %+v: %w", diff, ErrMissingColumns)
		}

		task, err := makeTaskForDiff(&diff)
		if err != nil {
			return nil, err
		}
		totalTasks++

		ret = append(ret, task)
	}

	// Create any needed "success file" tasks
	for successDirectory, td := range successForDirectory.m {
		successPath := fmt.Sprintf("%s/%s", successDirectory, successFilename)
		data := SuccessData{
			File: makeDestination(successPath),
		}
		body, err := json.Marshal(data)
		if err != nil {
			return nil, fmt.Errorf("%s: failed to serialize %+v: %w", successPath, data, err)
		}
		bodyStr := string(body)
		totalDependencies := td // copy to get new address each time

		ret = append(ret, parade.TaskData{
			ID:                parade.TaskID(makeSuccessTaskID(successPath)),
			Action:            TouchAction,
			Body:              &bodyStr,
			StatusCode:        parade.TaskPending,
			MaxTries:          &numTries,
			TotalDependencies: &totalDependencies,
		})
		totalTasks++
	}

	finishedTask.TotalDependencies = &totalTasks
	ret = append(ret, finishedTask)

	return ret, nil
}
