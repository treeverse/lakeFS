package export_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/go-test/deep"
	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/export"
	"github.com/treeverse/lakefs/parade"
)

// directDependencies describes the direct dependencies of task IDs.
type directDependencies interface {
	getDependencies(id parade.TaskID) []parade.TaskID
}

type tasksDependencies map[parade.TaskID][]parade.TaskID

func (td tasksDependencies) getDependencies(id parade.TaskID) []parade.TaskID {
	ret, ok := td[id]
	if !ok {
		return nil
	}
	return ret
}

func makeTasksDependencies(tasks []parade.TaskData) directDependencies {
	ret := make(tasksDependencies)
	for _, task := range tasks {
		ret[task.ID] = task.ToSignalAfter
	}
	return ret
}

// allDependencies allows O(1) indirect dependency checking of task IDs.
type allDependencies interface {
	depends(before, after parade.TaskID) bool
}

type allDependenciesStaticSet map[parade.TaskID]map[parade.TaskID]struct{}

// allDependenciesSet is a memoizing allDependencies.  It makes everything run in ~quadratic
// time, avoid using for large data.
type allDependenciesSet struct {
	dd directDependencies
	s  allDependenciesStaticSet
}

func (ads *allDependenciesSet) getAndCacheDependencies(before parade.TaskID) map[parade.TaskID]struct{} {
	deps, ok := ads.s[before]
	if !ok {
		deps = make(map[parade.TaskID]struct{})
		ads.s[before] = deps
		for _, id := range ads.dd.getDependencies(before) {
			deps[id] = struct{}{}
			indirect := ads.getAndCacheDependencies(id)
			for after := range indirect {
				deps[after] = struct{}{}
			}
		}
	}
	return deps
}

func (ads *allDependenciesSet) depends(before, after parade.TaskID) bool {
	deps := ads.getAndCacheDependencies(before)
	_, ok := deps[after]
	return ok
}

func makeAllDependencies(dd directDependencies) allDependencies {
	return &allDependenciesSet{dd: dd, s: make(map[parade.TaskID]map[parade.TaskID]struct{})}
}

func validateAllAfter(t *testing.T, d allDependencies, typ string, before parade.TaskID, afters []parade.TaskID) {
	t.Helper()
	for _, after := range afters {
		if !d.depends(before, after) {
			t.Errorf("missing %s dependency %s->%s", typ, before, after)
		}
	}
}

func cleanup(tasks []parade.TaskData) []parade.TaskData {
	ret := make([]parade.TaskData, len(tasks))
	for i, t := range tasks {
		c := t
		c.MaxTries = nil
		ret[i] = c
	}
	return ret
}

type taskPtrs []*parade.TaskData

func getTasks(pred func(t *parade.TaskData) bool, tasks []parade.TaskData) taskPtrs {
	ret := taskPtrs{}
	for _, t := range tasks {
		if pred(&t) {
			c := t
			ret = append(ret, &c)
		}
	}
	return ret
}

func isDone(t *parade.TaskData) bool {
	return t.Action == export.DoneAction
}

func isCopy(t *parade.TaskData) bool {
	return t.Action == export.CopyActipn
}

func isDelete(t *parade.TaskData) bool {
	return t.Action == export.DeleteAction
}

func isTouch(t *parade.TaskData) bool {
	return t.Action == export.TouchAction
}

func isUnknown(t *parade.TaskData) bool {
	return !isDone(t) && !isCopy(t) && !isDelete(t) && !isTouch(t)
}

var zero int = 0
var one int = 1

func TestTasksGenerator_Empty(t *testing.T) {
	tasks, err := export.GenerateTasksFromDiffs(
		"empty",
		"testfs://prefix/",
		catalog.Differences{},
		func(_ string) bool { return true })
	if err != nil {
		t.Fatalf("failed to GenerateTasksFromDiffs: %s", err)
	}

	copyTasks := getTasks(isCopy, tasks)
	if diffs := deep.Equal(taskPtrs{}, copyTasks); diffs != nil {
		t.Error("unexpected copy tasks", diffs)
	}
	deleteTasks := getTasks(isDelete, tasks)
	if diffs := deep.Equal(taskPtrs{}, deleteTasks); diffs != nil {
		t.Error("unexpected delete tasks", diffs)
	}
	successTasks := getTasks(isTouch, tasks)
	if diffs := deep.Equal(taskPtrs{}, successTasks); diffs != nil {
		t.Error("unexpected success tasks", diffs)
	}
	doneTasks := getTasks(isDone, tasks)
	if diffs := deep.Equal(taskPtrs{
		&parade.TaskData{ID: "empty:finished", Action: export.DoneAction, StatusCode: parade.TaskPending, MaxTries: &one, TotalDependencies: &zero},
	}, doneTasks); diffs != nil {
		t.Error("unexpected done tasks", diffs)
	}
	unknownTasks := getTasks(isUnknown, tasks)
	if diffs := deep.Equal(taskPtrs{}, unknownTasks); diffs != nil {
		t.Error("unknown tasks", diffs)
	}
}

func toJSON(t testing.TB, v interface{}) *string {
	j, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("failed to serialize %+v to JSON: %s", v, err)
	}
	s := string(j)
	return &s
}

func TestTasksGenerator_Simple(t *testing.T) {
	catalogDiffs := catalog.Differences{{
		Type:  catalog.DifferenceTypeAdded,
		Entry: catalog.Entry{Path: "add1", PhysicalAddress: "/add1"},
	}, {
		Type:  catalog.DifferenceTypeChanged,
		Entry: catalog.Entry{Path: "change1", PhysicalAddress: "/change1"},
	}, {
		Type:  catalog.DifferenceTypeRemoved,
		Entry: catalog.Entry{Path: "remove1", PhysicalAddress: "/remove1"},
	}}
	tasksWithIDs, err := export.GenerateTasksFromDiffs(
		"simple",
		"testfs://prefix/",
		catalogDiffs,
		func(_ string) bool { return false })
	if err != nil {
		t.Fatalf("failed to GenerateTasksFromDiffs: %s", err)
	}

	tasks := cleanup(tasksWithIDs)

	copyTasks := getTasks(isCopy, tasks)
	if diffs := deep.Equal(taskPtrs{
		&parade.TaskData{
			ID:     "simple:copy:/add1",
			Action: export.CopyActipn,
			Body: toJSON(t, export.CopyData{
				From: "/add1",
				To:   "testfs://prefix/add1",
			}),
			StatusCode:        parade.TaskPending,
			TotalDependencies: &one,
			ToSignalAfter:     []parade.TaskID{"simple:finished"},
		},
		&parade.TaskData{
			ID:     "simple:copy:/change1",
			Action: export.CopyActipn,
			Body: toJSON(t, export.CopyData{
				From: "/change1",
				To:   "testfs://prefix/change1",
			}),
			StatusCode:        parade.TaskPending,
			TotalDependencies: &one,
			ToSignalAfter:     []parade.TaskID{"simple:finished"},
		},
	}, copyTasks); diffs != nil {
		t.Error("unexpected copy tasks", diffs)
	}
	deleteTasks := getTasks(isDelete, tasks)
	if diffs := deep.Equal(taskPtrs{
		&parade.TaskData{
			ID:     "simple:delete:/remove1",
			Action: export.DeleteAction,
			Body: toJSON(t, export.DeleteData{
				File: "testfs://prefix/remove1",
			}),
			StatusCode:        parade.TaskPending,
			TotalDependencies: &one,
			ToSignalAfter:     []parade.TaskID{"simple:finished"},
		}}, deleteTasks); diffs != nil {
		t.Error("unexpected delete tasks", diffs)
	}
	successTasks := getTasks(isTouch, tasks)
	if diffs := deep.Equal(taskPtrs{}, successTasks); diffs != nil {
		t.Error("unexpected success tasks", diffs)
	}
	doneTasks := getTasks(isDone, tasks)
	totalDeps := len(catalogDiffs)
	if diffs := deep.Equal(taskPtrs{
		&parade.TaskData{ID: "simple:finished", Action: export.DoneAction, StatusCode: parade.TaskPending, TotalDependencies: &totalDeps},
	}, doneTasks); diffs != nil {
		t.Error("unexpected done tasks", diffs)
	}
	unknownTasks := getTasks(isUnknown, tasks)
	if diffs := deep.Equal(taskPtrs{}, unknownTasks); diffs != nil {
		t.Error("unknown tasks", diffs)
	}
}

func TestTasksGenerator_SuccessFiles(t *testing.T) {
	catalogDiffs := catalog.Differences{{
		Type:  catalog.DifferenceTypeRemoved,
		Entry: catalog.Entry{Path: "a/success/1", PhysicalAddress: "/remove1"},
	}, {
		Type:  catalog.DifferenceTypeRemoved,
		Entry: catalog.Entry{Path: "a/plain/1", PhysicalAddress: "/remove2"},
	}, {
		Type:  catalog.DifferenceTypeRemoved,
		Entry: catalog.Entry{Path: "a/success/sub/success/11", PhysicalAddress: "/remove11"},
	}, {
		Type:  catalog.DifferenceTypeRemoved,
		Entry: catalog.Entry{Path: "a/success/sub/success/12", PhysicalAddress: "/remove12"},
	}, {
		Type:  catalog.DifferenceTypeRemoved,
		Entry: catalog.Entry{Path: "b/success/1", PhysicalAddress: "/remove3"},
	}, {
		Type:  catalog.DifferenceTypeRemoved,
		Entry: catalog.Entry{Path: "a/success/2", PhysicalAddress: "/remove4"},
	}, {
		Type:  catalog.DifferenceTypeRemoved,
		Entry: catalog.Entry{Path: "a/plain/2", PhysicalAddress: "/remove5"},
	}}

	expectedDeps := []struct {
		before, after parade.TaskID
		avoid         bool
	}{
		{before: "foo:delete:/remove1", after: "foo:make-success:a/success"},
		{before: "foo:delete:/remove1", after: "foo:finished"},

		{before: "foo:delete:/remove2", after: "foo:make-success:a/plain", avoid: true},
		{before: "foo:delete:/remove2", after: "foo:finished"},

		{before: "foo:delete:/remove11", after: "foo:make-success:a/success/sub/success"},
		{before: "foo:delete:/remove11", after: "foo:finished"},

		{before: "foo:delete:/remove12", after: "foo:make-success:a/success/sub/success"},
		{before: "foo:delete:/remove12", after: "foo:finished"},

		{before: "foo:make-success:a/success/sub/success", after: "foo:make-success:a/success"},

		{before: "foo:delete:/remove3", after: "foo:make-success:b/success"},
		{before: "foo:delete:/remove3", after: "foo:finished"},
		{before: "foo:delete:/remove4", after: "foo:make-success:a/success"},
		{before: "foo:delete:/remove4", after: "foo:finished"},
		{before: "foo:delete:/remove5", after: "foo:make-success:a/plain", avoid: true},
		{before: "foo:delete:/remove5", after: "foo:finished"},
	}
	tasksWithIDs, err := export.GenerateTasksFromDiffs(
		"foo",
		"testfs://prefix/",
		catalogDiffs,
		func(path string) bool { return strings.HasSuffix(path, "success") },
	)
	if err != nil {
		t.Fatalf("failed to GenerateTasksFromDiffs: %s", err)
	}

	for i, task := range tasksWithIDs {
		t.Logf("task %d: %+v", i, task)
	}

	allDeps := makeAllDependencies(makeTasksDependencies(tasksWithIDs))

	for _, expectedDep := range expectedDeps {
		found := allDeps.depends(expectedDep.before, expectedDep.after)
		if found == expectedDep.avoid {
			neg := ""
			if found {
				neg = "not "
			}
			t.Errorf("expected %s %sto be after %s", expectedDep.after, neg, expectedDep.before)
		}
	}

	// Verify the right number of dependencies appeared.  Above we verified all indirect
	// dependencies.  Direct dependencies exactly form a tree, so every task signals one
	// other task... except for the finishing task.
	for _, task := range tasksWithIDs {
		if task.Action != export.DoneAction && len(task.ToSignalAfter) != 1 {
			t.Errorf("expected single task to signal after %+v", task)
		}
		if task.Action == export.DoneAction && len(task.ToSignalAfter) != 0 {
			t.Errorf("expected no tasks to signal after %+v", task)
		}
	}
}
