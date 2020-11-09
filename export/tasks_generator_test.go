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

func TestDirMatchCache(t *testing.T) {
	type L struct {
		path            string
		expectedDirName string
		expectedOK      bool
	}
	cases := []struct {
		name     string
		pred     func(path string) bool
		lookups  []L
		numCalls int
	}{
		{
			name: "no delimiter",
			pred: func(path string) bool { return strings.HasSuffix(path, "match") },
			lookups: []L{
				{"a", "", false}, // +1 calls
				{"", "", false},  // +0 calls
			},
			numCalls: 1,
		},
		{
			name: "leaves",
			pred: func(path string) bool { return strings.HasSuffix(path, "match") },
			lookups: []L{
				{"a/b/match", "", false},                   // +3 calls
				{"a/b/match/c", "a/b/match", true},         // +1 call
				{"a/b/match/c/d/e/f/g", "a/b/match", true}, // +4 calls
				{"", "", false},                            // +0 calls
				{"a/match/b/match", "a/match", true},       // +2 calls
			},
			numCalls: 10,
		},
		{
			name: "repeat",
			pred: func(path string) bool { return strings.HasSuffix(path, "match") },
			lookups: []L{
				{"a/b/match/c", "a/b/match", true}, // +1 call
				{"a/b/match/c", "a/b/match", true}, // +0 calls
				{"a/b/match/d", "a/b/match", true}, // +0 calls
			},
			numCalls: 1,
		},
		{
			name: "empty-matches",
			pred: func(path string) bool {
				// Always match, if only at the top-level
				return strings.HasSuffix(path, "x") || path == ""
			},
			lookups: []L{
				{"", "", true},            // +1 call
				{"a/b/c/file", "", true},  // +3 calls
				{"a/x/file", "a/x", true}, // +1 call
			},
			numCalls: 5,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			calls := make([]string, 0)
			dmc := export.NewDirMatchCache(func(path string) bool {
				calls = append(calls, path)
				return c.pred(path)
			})
			for _, l := range c.lookups {
				dir, ok := dmc.Lookup(l.path)
				if l.expectedOK != ok || ok && l.expectedDirName != dir {
					t.Errorf("expected %s, %v but got %s, %v",
						l.expectedDirName, l.expectedOK, dir, ok)
				}
			}
			if c.numCalls > 0 && c.numCalls != len(calls) {
				t.Errorf("expected %d calls but performed these %d: %s", c.numCalls, len(calls), strings.Join(calls, ", "))
			}
		})
	}
}

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
	return t.Action == export.CopyAction
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
	gen := export.NewTasksGenerator("empty", "testfs://prefix/", func(_ string) bool { return true }, nil)

	tasks, err := gen.Finish()
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
		Entry: catalog.Entry{Path: "add1", PhysicalAddress: "add1"},
	}, {
		Type:  catalog.DifferenceTypeChanged,
		Entry: catalog.Entry{Path: "change1", PhysicalAddress: "change1"},
	}, {
		Type:  catalog.DifferenceTypeRemoved,
		Entry: catalog.Entry{Path: "remove1", PhysicalAddress: "remove1"},
	}}
	gen := export.NewTasksGenerator(
		"simple",
		"testfs://prefix/",
		func(_ string) bool { return false }, nil)
	tasksWithIDs, err := gen.Add(catalogDiffs)
	if err != nil {
		t.Fatalf("failed to add tasks: %s", err)
	}
	finishTasks, err := gen.Finish()
	if err != nil {
		t.Fatalf("failed to finish generating tasks: %s", err)
	}
	tasksWithIDs = append(tasksWithIDs, finishTasks...)
	tasks := cleanup(tasksWithIDs)

	copyTasks := getTasks(isCopy, tasks)
	idGen := export.TaskIDGenerator("simple")
	if diffs := deep.Equal(taskPtrs{
		&parade.TaskData{
			ID:     idGen.CopyTaskID("add1"),
			Action: export.CopyAction,
			Body: toJSON(t, export.CopyData{
				From: "add1",
				To:   "testfs://prefix/add1",
			}),
			StatusCode:        parade.TaskPending,
			TotalDependencies: &zero,
			ToSignalAfter:     []parade.TaskID{"simple:finished"},
		},
		&parade.TaskData{
			ID:     idGen.CopyTaskID("change1"),
			Action: export.CopyAction,
			Body: toJSON(t, export.CopyData{
				From: "change1",
				To:   "testfs://prefix/change1",
			}),
			StatusCode:        parade.TaskPending,
			TotalDependencies: &zero,
			ToSignalAfter:     []parade.TaskID{"simple:finished"},
		},
	}, copyTasks); diffs != nil {
		t.Error("unexpected copy tasks", diffs)
	}
	deleteTasks := getTasks(isDelete, tasks)
	if diffs := deep.Equal(taskPtrs{
		&parade.TaskData{
			ID:     idGen.DeleteTaskID("remove1"),
			Action: export.DeleteAction,
			Body: toJSON(t, export.DeleteData{
				File: "testfs://prefix/remove1",
			}),
			StatusCode:        parade.TaskPending,
			TotalDependencies: &zero,
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
		Entry: catalog.Entry{Path: "a/success/1", PhysicalAddress: "remove1"},
	}, {
		Type:  catalog.DifferenceTypeRemoved,
		Entry: catalog.Entry{Path: "a/plain/1", PhysicalAddress: "remove2"},
	}, {
		Type:  catalog.DifferenceTypeRemoved,
		Entry: catalog.Entry{Path: "a/success/sub/success/11", PhysicalAddress: "remove11"},
	}, {
		Type:  catalog.DifferenceTypeRemoved,
		Entry: catalog.Entry{Path: "a/success/sub/success/12", PhysicalAddress: "remove12"},
	}, {
		Type:  catalog.DifferenceTypeRemoved,
		Entry: catalog.Entry{Path: "b/success/1", PhysicalAddress: "remove3"},
	}, {
		Type:  catalog.DifferenceTypeRemoved,
		Entry: catalog.Entry{Path: "a/success/2", PhysicalAddress: "remove4"},
	}, {
		Type:  catalog.DifferenceTypeRemoved,
		Entry: catalog.Entry{Path: "a/plain/2", PhysicalAddress: "remove5"},
	}}
	idGen := export.TaskIDGenerator("foo")
	expectedDeps := []struct {
		before, after parade.TaskID
		avoid         bool
	}{
		{before: idGen.DeleteTaskID("a/success/1"), after: "foo:make-success:a/success"},
		{before: idGen.DeleteTaskID("a/success/1"), after: "foo:finished"},

		{before: idGen.DeleteTaskID("a/plain/1"), after: "foo:make-success:a/plain", avoid: true},
		{before: idGen.DeleteTaskID("a/plain/1"), after: "foo:finished"},

		{before: idGen.DeleteTaskID("a/success/sub/success/11"), after: "foo:make-success:a/success/sub/success"},
		{before: idGen.DeleteTaskID("a/success/sub/success/11"), after: "foo:finished"},

		{before: idGen.DeleteTaskID("a/success/sub/success/12"), after: "foo:make-success:a/success/sub/success"},
		{before: idGen.DeleteTaskID("a/success/sub/success/12"), after: "foo:finished"},

		{before: "foo:make-success:a/success/sub/success", after: "foo:make-success:a/success"},

		{before: idGen.DeleteTaskID("b/success/1"), after: "foo:make-success:b/success"},
		{before: idGen.DeleteTaskID("b/success/1"), after: "foo:finished"},
		{before: idGen.DeleteTaskID("a/success/2"), after: "foo:make-success:a/success"},
		{before: idGen.DeleteTaskID("a/success/2"), after: "foo:finished"},
		{before: idGen.DeleteTaskID("a/plain/2"), after: "foo:make-success:a/plain", avoid: true},
		{before: idGen.DeleteTaskID("a/plain/2"), after: "foo:finished"},
	}
	gen := export.NewTasksGenerator(
		"foo",
		"testfs://prefix/",
		func(path string) bool { return strings.HasSuffix(path, "success") },
		nil,
	)

	tasksWithIDs := make([]parade.TaskData, 0, len(catalogDiffs))

	for o := 0; o < len(catalogDiffs); o += 3 {
		end := o + 3
		if end > len(catalogDiffs) {
			end = len(catalogDiffs)
		}
		slice := catalogDiffs[o:end]
		moreTasks, err := gen.Add(slice)
		if err != nil {
			t.Fatalf("failed to add tasks %d..%d: %+v: %s", o, end, catalogDiffs[o:o+3], err)
		}
		tasksWithIDs = append(tasksWithIDs, moreTasks...)
	}
	moreTasks, err := gen.Finish()
	if err != nil {
		t.Fatalf("failed to finish generating tasks: %s", err)
	}
	tasksWithIDs = append(tasksWithIDs, moreTasks...)

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
