package parade_test

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-test/deep"
	"github.com/jackc/pgx/v4"
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/treeverse/lakefs/parade"
	"github.com/treeverse/lakefs/parade/testutil"

	"github.com/jmoiron/sqlx"
	"github.com/ory/dockertest/v3"
)

var (
	pool        *dockertest.Pool
	databaseURI string
	db          *sqlx.DB

	postgresUrl = flag.String("postgres-url", "", "Postgres connection string.  If unset, run a Postgres in a Docker container.  If set, should have ddl.sql already loaded.")
	parallelism = flag.Int("parallelism", 16, "Number of concurrent client worker goroutines.")
	bulk        = flag.Int("bulk", 2_000, "Number of tasks to acquire at once in each client goroutine.")
	taskFactor  = flag.Int("task-factor", 20_000, "Scale benchmark N by this many tasks")
	numShards   = flag.Int("num-shards", 400, "Number of intermediate fan-in shards")
)

func TestMain(m *testing.M) {
	var err error
	flag.Parse()
	pool, err = dockertest.NewPool("")
	if err != nil {
		log.Fatalf("could not connect to Docker: %s", err)
	}
	var dbCleanup func()
	databaseURI, dbCleanup = testutil.RunDBInstance(pool, *postgresUrl)
	defer dbCleanup() // In case we don't reach the cleanup action.
	db = sqlx.MustConnect("pgx", databaseURI)
	defer db.Close()
	code := m.Run()
	if _, ok := os.LookupEnv("GOTEST_KEEP_DB"); !ok && dbCleanup != nil {
		dbCleanup() // os.Exit() below won't call the defered cleanup, do it now.
	}
	os.Exit(code)
}

// wrapper derives a prefix from t.Name and uses it to provide namespaced access to parade on DB
// db as well as a simple error-reporting inserter.
type wrapper struct {
	t  testing.TB
	db *sqlx.DB
}

func (w wrapper) prefix(s string) string {
	return fmt.Sprintf("%s.%s", w.t.Name(), s)
}

func (w wrapper) strip(s string) string {
	return strings.TrimPrefix(s, w.t.Name()+".")
}

func (w wrapper) prefixTask(id parade.TaskID) parade.TaskID {
	return parade.TaskID(w.prefix(string(id)))
}

func (w wrapper) stripTask(id parade.TaskID) parade.TaskID {
	return parade.TaskID(w.strip(string(id)))
}

func (w wrapper) prefixActor(actor parade.ActorID) parade.ActorID {
	return parade.ActorID(w.prefix(string(actor)))
}

func (w wrapper) stripActor(actor parade.TaskID) parade.ActorID {
	return parade.ActorID(w.strip(string(actor)))
}

func (w wrapper) insertTasks(tasks []parade.TaskData) func() {
	w.t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, databaseURI)
	if err != nil {
		w.t.Fatalf("pgx.Connect: %s", err)
	}
	defer conn.Close(ctx)

	prefixedTasks := make([]parade.TaskData, len(tasks))
	for i := 0; i < len(tasks); i++ {
		copy := tasks[i]
		copy.ID = w.prefixTask(copy.ID)
		copy.Action = w.prefix(copy.Action)
		copy.ActorID = w.prefixActor(copy.ActorID)
		if copy.StatusCode == "" {
			copy.StatusCode = "pending"
		}
		toSignal := make([]parade.TaskID, len(copy.ToSignal))
		for j := 0; j < len(toSignal); j++ {
			toSignal[j] = w.prefixTask(copy.ToSignal[j])
		}
		copy.ToSignal = toSignal
		prefixedTasks[i] = copy
	}
	err = parade.InsertTasks(ctx, conn, &parade.TaskDataIterator{Data: prefixedTasks})
	if err != nil {
		w.t.Fatalf("InsertTasks: %s", err)
	}

	// Create cleanup callback.  Compute the ids now, tasks may change later.
	ids := make([]parade.TaskID, 0, len(tasks))
	for _, task := range tasks {
		ids = append(ids, task.ID)
	}
	return func() { w.deleteTasks(ids) }
}

func (w wrapper) deleteTasks(ids []parade.TaskID) error {
	prefixedIds := make([]parade.TaskID, len(ids))
	for i := 0; i < len(ids); i++ {
		prefixedIds[i] = w.prefixTask(ids[i])
	}
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, databaseURI)
	if err != nil {
		return fmt.Errorf("connect to DB: %w", err)
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("BEGIN: %w", err)
	}
	defer func() {
		if tx != nil {
			tx.Rollback(ctx)
		}
	}()

	if err = parade.DeleteTasks(ctx, tx, prefixedIds); err != nil {
		return err
	}

	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("COMMIT: %w", err)
	}
	tx = nil
	return nil
}

func (w wrapper) returnTask(taskId parade.TaskID, token parade.PerformanceToken, resultStatus string, resultStatusCode parade.TaskStatusCodeValue) error {
	return parade.ReturnTask(w.db, w.prefixTask(taskId), token, resultStatus, resultStatusCode)
}

func (w wrapper) ownTasks(actorId parade.ActorID, maxTasks int, actions []string, maxDuration *time.Duration) ([]parade.OwnedTaskData, error) {
	prefixedActions := make([]string, len(actions))
	for i, action := range actions {
		prefixedActions[i] = w.prefix(action)
	}
	tasks, err := parade.OwnTasks(w.db, actorId, maxTasks, prefixedActions, maxDuration)
	if tasks != nil {
		for i := 0; i < len(tasks); i++ {
			task := &tasks[i]
			task.ID = w.stripTask(task.ID)
		}
	}
	return tasks, err
}

func stringAddr(s string) *string {
	return &s
}

func performanceTokenAddr(p parade.PerformanceToken) *parade.PerformanceToken {
	return &p
}

func intAddr(i int) *int {
	return &i
}

func TestTaskDataIterator_Empty(t *testing.T) {
	it := parade.TaskDataIterator{Data: []parade.TaskData{}}
	if it.Err() != nil {
		t.Errorf("expected empty new iterator to have no errors, got %s", it.Err())
	}
	if it.Next() {
		t.Errorf("expected empty new iterator %+v not to advance", it)
	}
	if it.Err() != nil {
		t.Errorf("advanced empty new iterator to have no errors, got %s", it.Err())
	}
	if it.Next() {
		t.Errorf("expected advanced empty new iterator %+v not to advance", it)
	}
	if !errors.Is(it.Err(), parade.ErrNoMoreData) {
		t.Errorf("expected twice-advanced iterator to raise NoMoreDataError, got %s", it.Err())
	}
}

func TestTaskDataIterator_Values(t *testing.T) {
	now := time.Now()
	tasks := []parade.TaskData{
		{ID: "000", Action: "zero", StatusCode: "enum values enforced on DB"},
		{ID: "111", Action: "frob", Body: stringAddr("1"), Status: stringAddr("state"),
			StatusCode: "pending",
			NumTries:   11, MaxTries: intAddr(17),
			TotalDependencies: intAddr(9),
			ToSignal:          []parade.TaskID{parade.TaskID("foo"), parade.TaskID("bar")},
			ActorID:           parade.ActorID("actor"), ActionDeadline: &now,
			PerformanceToken:  performanceTokenAddr(parade.PerformanceToken{}),
			FinishChannelName: stringAddr("done"),
		},
	}
	it := parade.TaskDataIterator{Data: tasks}

	for index, task := range tasks {
		if it.Err() != nil {
			t.Errorf("expected iterator to be OK at index %d, got error %s", index, it.Err())
		}
		if !it.Next() {
			t.Errorf("expected to advance iterator %+v at index %d", it, index)
		}
		values, err := it.Values()
		if err != nil {
			t.Errorf("expected to values at index %d, got error %s", index, err)
		}
		toSignal := make([]string, len(task.ToSignal))
		for i := 0; i < len(task.ToSignal); i++ {
			toSignal[i] = string(task.ToSignal[i])
		}
		if diffs := deep.Equal(
			[]interface{}{
				task.ID, task.Action, task.Body, task.Status, task.StatusCode,
				task.NumTries, task.MaxTries,
				task.TotalDependencies, toSignal,
				task.ActorID, task.ActionDeadline,
				task.PerformanceToken, task.FinishChannelName,
			}, values); diffs != nil {
			t.Errorf("got other values at index %d than expected: %s", index, diffs)
		}
	}
	if it.Next() {
		t.Errorf("expected iterator %+v to end after tasks done", it)
	}
	if _, err := it.Values(); !errors.Is(err, parade.ErrNoMoreData) {
		t.Errorf("expected NoMoreData after iterator done, got %s", err)
	}
	if !errors.Is(it.Err(), parade.ErrNoMoreData) {
		t.Errorf("expected iterator Err() to repeat NoMoreDataError, got %s", it.Err())
	}
	if it.Next() {
		t.Errorf("expected iterator %+v to end-and-error after advanced past tasks done", it)
	}
}

func TestOwn(t *testing.T) {
	w := wrapper{t, db}

	cleanup := w.insertTasks([]parade.TaskData{
		{ID: "000", Action: "never"},
		{ID: "111", Action: "frob"},
		{ID: "123", Action: "broz"},
		{ID: "222", Action: "broz"},
	})
	defer cleanup()
	tasks, err := w.ownTasks(parade.ActorID("tester"), 2, []string{"frob", "broz"}, nil)
	if err != nil {
		t.Errorf("first own_tasks query: %s", err)
	}
	if len(tasks) != 2 {
		t.Errorf("expected first OwnTasks to return 2 tasks but got %d: %+v", len(tasks), tasks)
	}
	gotTasks := tasks

	tasks, err = w.ownTasks(parade.ActorID("tester-two"), 2, []string{"frob", "broz"}, nil)
	if err != nil {
		t.Errorf("second own_tasks query: %s", err)
	}
	if len(tasks) != 1 {
		t.Errorf("expected second OwnTasks to return 1 task but got %d: %+v", len(tasks), tasks)
	}
	gotTasks = append(gotTasks, tasks...)

	gotIds := make([]parade.TaskID, 0, len(gotTasks))
	for _, got := range gotTasks {
		gotIds = append(gotIds, got.ID)
	}
	sort.Sort(testutil.TaskIdSlice(gotIds))
	if diffs := deep.Equal([]parade.TaskID{"111", "123", "222"}, gotIds); diffs != nil {
		t.Errorf("expected other task IDs: %s", diffs)
	}
}

func TestOwnBody(t *testing.T) {
	w := wrapper{t, db}

	val := "\"the quick brown fox jumps over the lazy dog\""

	cleanup := w.insertTasks([]parade.TaskData{
		{ID: "body", Action: "yes", Body: &val},
		{ID: "nobody", Action: "no"},
	})
	defer cleanup()

	tasks, err := w.ownTasks(parade.ActorID("somebody"), 2, []string{"yes", "no"}, nil)
	if err != nil {
		t.Fatalf("own tasks: %s", err)
	}
	if len(tasks) != 2 {
		t.Fatalf("expected to own 2 tasks but got %+v", tasks)
	}
	body, nobody := tasks[0], tasks[1]
	if body.ID != "body" {
		body, nobody = nobody, body
	}

	if nobody.Body != nil {
		t.Errorf("unexpected body in task %+v", nobody)
	}
	if body.Body == nil || *body.Body != val {
		t.Errorf("expected body \"%s\" in task %+v", val, body)
	}
}

func TestOwnAfterDeadlineElapsed(t *testing.T) {
	second := 1 * time.Second
	w := wrapper{t, db}

	cleanup := w.insertTasks([]parade.TaskData{
		{ID: "111", Action: "frob"},
	})
	defer cleanup()

	_, err := w.ownTasks(parade.ActorID("tortoise"), 1, []string{"frob"}, &second)
	if err != nil {
		t.Fatalf("failed to setup tortoise task ownership: %s", err)
	}

	fastTasks, err := w.ownTasks(parade.ActorID("hare"), 1, []string{"frob"}, &second)
	if err != nil {
		t.Fatalf("failed to request fast task ownership: %s", err)
	}
	if len(fastTasks) != 0 {
		t.Errorf("expected immedidate hare task ownership to return nothing but got %+v", fastTasks)
	}

	time.Sleep(2 * time.Second)
	fastTasks, err = w.ownTasks(parade.ActorID("hare"), 1, []string{"frob"}, &second)
	if err != nil {
		t.Fatalf("failed to request fast task ownership after sleeping: %s", err)
	}
	if len(fastTasks) != 1 || fastTasks[0].ID != "111" {
		t.Errorf("expected eventual hare task ownership to return task \"111\" but got tasks %+v", fastTasks)
	}
}

func TestReturnTask_DirectlyAndRetry(t *testing.T) {
	w := wrapper{t, db}

	cleanup := w.insertTasks([]parade.TaskData{
		{ID: "111", Action: "frob"},
		{ID: "123", Action: "broz"},
		{ID: "222", Action: "broz"},
	})
	defer cleanup()

	tasks, err := w.ownTasks(parade.ActorID("foo"), 4, []string{"frob", "broz"}, nil)
	if err != nil {
		t.Fatalf("acquire all tasks: %s", err)
	}

	taskById := make(map[parade.TaskID]*parade.OwnedTaskData, len(tasks))
	for index := range tasks {
		taskById[tasks[index].ID] = &tasks[index]
	}

	if err = w.returnTask(taskById[parade.TaskID("111")].ID, taskById[parade.TaskID("111")].Token, "done", parade.TaskCompleted); err != nil {
		t.Errorf("return task 111: %s", err)
	}

	if err = w.returnTask(taskById[parade.TaskID("111")].ID, taskById[parade.TaskID("111")].Token, "done", parade.TaskCompleted); !errors.Is(err, parade.ErrInvalidToken) {
		t.Errorf("expected second attempt to return task 111 to fail with InvalidTokenError, got %s", err)
	}

	// Now attempt to return a task to in-progress state.
	if err = w.returnTask(taskById[parade.TaskID("123")].ID, taskById[parade.TaskID("123")].Token, "try-again", parade.TaskPending); err != nil {
		t.Errorf("return task 123 (%+v) for another round: %s", taskById[parade.TaskID("123")], err)
	}
	moreTasks, err := w.ownTasks(parade.ActorID("foo"), 4, []string{"frob", "broz"}, nil)
	if err != nil {
		t.Fatalf("re-acquire task 123: %s", err)
	}
	if len(moreTasks) != 1 || moreTasks[0].ID != parade.TaskID("123") {
		t.Errorf("expected to receive only task 123 but got tasks %+v", moreTasks)
	}
}

func TestReturnTask_RetryMulti(t *testing.T) {
	w := wrapper{t, db}

	maxTries := 7
	lifetime := 250 * time.Millisecond

	cleanup := w.insertTasks([]parade.TaskData{
		{ID: "111", Action: "frob", MaxTries: &maxTries},
	})
	defer cleanup()

	for i := 0; i < maxTries; i++ {
		tasks, err := w.ownTasks(parade.ActorID("foo"), 1, []string{"frob"}, &lifetime)
		if err != nil {
			t.Errorf("acquire task after %d/%d tries: %s", i, maxTries, err)
		}
		if len(tasks) != 1 {
			t.Fatalf("expected to own single task after %d/%d tries but got %+v", i, maxTries, tasks)
		}
		if i%2 == 0 {
			time.Sleep(2 * lifetime)
		} else {
			if err = w.returnTask(tasks[0].ID, tasks[0].Token, "retry", parade.TaskPending); err != nil {
				t.Fatalf("return task %+v after %d/%d tries: %s", tasks[0], i, maxTries, err)
			}
		}
	}

	tasks, err := w.ownTasks(parade.ActorID("foo"), 1, []string{"frob"}, &lifetime)
	if err != nil {
		t.Fatalf("re-acquire task failed: %s", err)
	}
	if len(tasks) != 0 {
		t.Errorf("expected not to receive any tasks (maxRetries)  but got tasks %+v", tasks)
	}
}

func TestDependencies(t *testing.T) {
	w := wrapper{t, db}

	id := func(n int) parade.TaskID {
		return parade.TaskID(fmt.Sprintf("number-%d", n))
	}
	makeBody := func(n int) *string {
		ret := fmt.Sprintf("%d", n)
		return &ret
	}
	parseBody := func(s string) int {
		ret, err := strconv.ParseInt(s, 10, 0)
		if err != nil {
			t.Fatalf("parse body %s: %s", s, err)
		}
		return int(ret)
	}

	// Tasks: take a limit number.  Create a task for each number from 1 to this limit
	// number.  Now add dependencies: each task can only be executed after all its divisors'
	// tasks have been executed.  This provides an interesting graph of dependencies that is
	// also easy to check.
	const num = 63
	taskData := make([]parade.TaskData, 0, num)
	for i := 1; i <= num; i++ {
		toSignal := make([]parade.TaskID, 0, i/20)
		for j := 2 * i; j <= num; j += i {
			toSignal = append(toSignal, id(j))
		}
		numDivisors := 0
		for k := 1; k <= i/2; k++ {
			if i%k == 0 {
				numDivisors++
			}
		}

		taskData = append(taskData, parade.TaskData{
			ID:                id(i),
			Action:            "div",
			Body:              makeBody(i),
			ToSignal:          toSignal,
			TotalDependencies: &numDivisors,
		})
	}
	cleanup := w.insertTasks(taskData)
	defer cleanup()

	doneSet := make(map[int]struct{}, num)

	for {
		tasks, err := w.ownTasks(parade.ActorID("foo"), 17, []string{"div"}, nil)
		if err != nil {
			t.Fatalf("acquire tasks with done %+v: %s", doneSet, err)
		}
		if len(tasks) == 0 {
			break
		}
		for _, task := range tasks {
			n := parseBody(*task.Body)
			doneSet[n] = struct{}{}
			for d := 1; d <= n/2; d++ {
				if n%d == 0 {
					if _, ok := doneSet[d]; !ok {
						t.Errorf("retrieved task %+v before task for its divisor %d", task, d)
					}
				}
			}
			if err = w.returnTask(task.ID, task.Token, "divided", parade.TaskCompleted); err != nil {
				t.Errorf("failed to complete task %+v: %s", task, err)
			}
		}
	}
	if len(doneSet) < num {
		t.Errorf("finished before processing all numbers up to %d: got just %+v", num, doneSet)
	}
}

func TestDeleteTasks(t *testing.T) {
	// Delete tasks requires whitebox testing, to ensure tasks really are deleted.
	w := wrapper{t, db}

	cleanup := w.insertTasks([]parade.TaskData{
		{ID: parade.TaskID("a0"), Action: "root", ToSignal: []parade.TaskID{"a1", "a3"}},
		{ID: parade.TaskID("a1"), Action: "dep", ToSignal: []parade.TaskID{"a2"}, TotalDependencies: intAddr(1)},
		{ID: parade.TaskID("a2"), Action: "dep", ToSignal: []parade.TaskID{"a3"}, TotalDependencies: intAddr(1)},
		{ID: parade.TaskID("a3"), Action: "leaf", TotalDependencies: intAddr(2)},

		{ID: parade.TaskID("b0"), Action: "root", ToSignal: []parade.TaskID{"b1"}},
		{ID: parade.TaskID("b1"), Action: "root-keep", ToSignal: []parade.TaskID{"b2"}},
		{ID: parade.TaskID("b2"), Action: "leaf", TotalDependencies: intAddr(2)},

		{ID: parade.TaskID("c0"), Action: "root", ToSignal: []parade.TaskID{"c1", "c2"}},
		{ID: parade.TaskID("c1"), Action: "dep", ToSignal: []parade.TaskID{"c3", "c4"}, TotalDependencies: intAddr(1)},
		{ID: parade.TaskID("c2"), Action: "dep", ToSignal: []parade.TaskID{"c4", "c5"}, TotalDependencies: intAddr(1)},
		{ID: parade.TaskID("c3"), Action: "dep", ToSignal: []parade.TaskID{"c5", "c6"}, TotalDependencies: intAddr(1)},
		{ID: parade.TaskID("c4"), Action: "leaf", TotalDependencies: intAddr(2)},
		{ID: parade.TaskID("c5"), Action: "leaf", TotalDependencies: intAddr(2)},
		{ID: parade.TaskID("c6"), Action: "leaf", TotalDependencies: intAddr(1)},
	})
	defer cleanup()

	type testCase struct {
		title             string
		casePrefix        string
		toDelete          []parade.TaskID
		expectedRemaining []parade.TaskID
	}
	cases := []testCase{
		{title: "chain with extra link", casePrefix: "a", toDelete: []parade.TaskID{"a0"}},
		{title: "delete only one dep", casePrefix: "b", toDelete: []parade.TaskID{"b0"}, expectedRemaining: []parade.TaskID{"b1", "b2"}},
		{title: "treelike", casePrefix: "c", toDelete: []parade.TaskID{"c0"}},
	}
	prefix := t.Name()
	for _, c := range cases {
		t.Run(c.title, func(t *testing.T) {
			casePrefix := fmt.Sprint(prefix, ".", c.casePrefix)
			if err := w.deleteTasks(c.toDelete); err != nil {
				t.Errorf("DeleteTasks failed: %s", err)
			}

			rows, err := w.db.Query(`SELECT id FROM tasks WHERE id LIKE format('%s%%', $1::text)`, casePrefix)
			if err != nil {
				t.Errorf("[I] select remaining IDs for prefix %s: %s", casePrefix, err)
			}

			defer func() {
				if err := rows.Close(); err != nil {
					t.Fatalf("[I] remaining ids iterator close: %s", err)
				}
			}()
			gotRemaining := make([]parade.TaskID, 0, len(c.expectedRemaining))
			for rows.Next() {
				var id parade.TaskID
				if err = rows.Scan(&id); err != nil {
					t.Errorf("[I] scan ID value: %s", err)
				}
				gotRemaining = append(gotRemaining, id)
			}
			sort.Sort(testutil.TaskIdSlice(gotRemaining))
			expectedRemaining := c.expectedRemaining
			if expectedRemaining == nil {
				expectedRemaining = []parade.TaskID{}
			}
			for i, e := range expectedRemaining {
				expectedRemaining[i] = w.prefixTask(e)
			}
			sort.Sort(testutil.TaskIdSlice(expectedRemaining))
			if diffs := deep.Equal(expectedRemaining, gotRemaining); diffs != nil {
				t.Errorf("left with other IDs than expected: %s", diffs)
			}
		})
	}
}

func TestNotification(t *testing.T) {
	ctx := context.Background()
	w := wrapper{t, db}

	type testCase struct {
		title      string
		id         parade.TaskID
		status     string
		statusCode parade.TaskStatusCodeValue
	}

	cases := []testCase{
		{"task aborted", parade.TaskID("111"), "b0rked!", parade.TaskAborted},
		{"task succeeded", parade.TaskID("222"), "yay!", parade.TaskCompleted},
	}

	for _, c := range cases {
		t.Run(c.title, func(t *testing.T) {
			cleanup := w.insertTasks([]parade.TaskData{
				{ID: c.id, Action: "frob"},
			})
			defer cleanup()

			tasks, err := w.ownTasks(parade.ActorID("foo"), 1, []string{"frob"}, nil)
			if err != nil {
				t.Fatalf("acquire task: %s", err)
			}
			if len(tasks) != 1 {
				t.Fatalf("expected to own single task but got %+v", tasks)
			}

			conn, err := pgx.Connect(ctx, databaseURI)
			if err != nil {
				t.Fatalf("pgx.Connect: %s", err)
			}
			defer conn.Close(ctx)

			type result struct {
				status     string
				statusCode parade.TaskStatusCodeValue
				err        error
			}
			ch := make(chan result)
			go func() {
				status, statusCode, err := parade.WaitForTask(ctx, conn, parade.TaskID("111"))
				ch <- result{status, statusCode, err}
			}()

			if err = w.returnTask(tasks[0].ID, tasks[0].Token, c.status, c.statusCode); err != nil {
				t.Fatalf("return task %+v: %s", tasks[0], err)
			}

			got := <-ch
			expected := result{c.status, c.statusCode, nil}
			if diffs := deep.Equal(expected, got); diffs != nil {
				t.Errorf("WaitForTask returned unexpected values: %s", diffs)
			}
		})
	}
}

func BenchmarkFanIn(b *testing.B) {
	numTasks := b.N * *taskFactor

	w := wrapper{b, db}

	id := func(n int) parade.TaskID {
		return parade.TaskID(fmt.Sprintf("in:%08d", n))
	}
	shardId := func(n int) parade.TaskID {
		return parade.TaskID(fmt.Sprintf("done:%05d", n))
	}

	tasks := make([]parade.TaskData, 0, numTasks+*numShards+1)
	totalShardDependencies := make([]int, *numShards)
	for i := 0; i < numTasks; i++ {
		shard := (i / 100) % *numShards
		toSignal := []parade.TaskID{shardId(shard)}
		tasks = append(tasks, parade.TaskData{ID: id(i), Action: "part", ToSignal: toSignal})
		totalShardDependencies[shard]++
	}

	toSignal := []parade.TaskID{"done"}
	for i := 0; i < *numShards; i++ {
		tasks = append(tasks, parade.TaskData{ID: shardId(i), Action: "spontaneous", ToSignal: toSignal, TotalDependencies: &totalShardDependencies[i]})
	}
	tasks = append(tasks, parade.TaskData{ID: "done", Action: "done", TotalDependencies: numShards})
	cleanup := w.insertTasks(tasks)

	defer cleanup()

	type result struct {
		count        int
		receivedDone bool
		err          error
	}
	resultCh := make([]chan result, *parallelism)
	for i := 0; i < *parallelism; i++ {
		resultCh[i] = make(chan result)
	}

	startTime := time.Now() // Cannot access Go benchmark timer, get it manually
	b.ResetTimer()
	for i := 0; i < *parallelism; i++ {
		go func(i int) {
			count := 0
			receivedDone := false
			for {
				size := *bulk + int(rand.Int31n(int32(*bulk/5))) - *bulk/10
				tasks, err := w.ownTasks(
					parade.ActorID(fmt.Sprintf("worker-%d", i)),
					size,
					[]string{"part", "spontaneous", "done"}, nil)
				if err != nil {
					resultCh[i] <- result{err: err}
					return
				}
				if len(tasks) == 0 {
					break
				}
				for _, task := range tasks {
					switch task.Action {
					case w.prefix("part"):
						count++
					case w.prefix("spontaneous"):
						// nothing, just reduce fan-in contention
					case w.prefix("done"):
						receivedDone = true
					default:
						resultCh[i] <- result{err: fmt.Errorf("weird action %s", task.Action)}
					}
					w.returnTask(task.ID, task.Token, "ok", parade.TaskCompleted)
				}
			}
			resultCh[i] <- result{count: count, receivedDone: receivedDone}
		}(i)
	}

	var total, numDone int
	for i := 0; i < *parallelism; i++ {
		r := <-resultCh[i]
		if r.err != nil {
			b.Errorf("goroutine %d failed: %s", i, r.err)
		}
		total += r.count
		if r.receivedDone {
			numDone++
		}
	}
	b.StopTimer()
	duration := time.Since(startTime)
	if total != numTasks {
		b.Errorf("expected %d tasks but processed %d", numTasks, total)
	}
	if numDone != 1 {
		b.Errorf("expected single goroutine to process \"done\" but got %d", numDone)
	}
	b.ReportMetric(float64(numTasks), "num_tasks")
	b.ReportMetric(float64(*parallelism), "num_goroutines")
	b.ReportMetric(float64(float64(numTasks)/float64(duration)*1e9), "tasks/sec")
}
