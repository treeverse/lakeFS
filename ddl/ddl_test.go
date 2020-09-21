package ddl_test

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
	"github.com/treeverse/parade/ddl"

	"github.com/jmoiron/sqlx"
	"github.com/ory/dockertest/v3"
)

const (
	dbContainerTimeoutSeconds = 10 * 60 // 10 min
	dbName                    = "parade_db"
)

var (
	pool        *dockertest.Pool
	databaseURI string
	db          *sqlx.DB

	postgresUrl = flag.String("postgres-url", "", "Postgres connection string.  If unset, run a Postgres in a Docker container.  If set, should have ddl.sql already loaded.")
	parallelism = flag.Int("parallelism", 16, "Number of concurrent client worker goroutines.")
	bulk        = flag.Int("bulk", 2_000, "Number of tasks to acquire at once in each client goroutine.")
	taskFactor  = flag.Int("task-factor", 20_000, "Scale benchmark N by this many tasks")
)

// taskIdSlice attaches the methods of sort.Interface to []TaskId.
type taskIdSlice []ddl.TaskId

func (p taskIdSlice) Len() int           { return len(p) }
func (p taskIdSlice) Less(i, j int) bool { return p[i] < p[j] }
func (p taskIdSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

// runDBInstance starts a test Postgres server inside container pool, and returns a connection
// URI and a closer function.
func runDBInstance(pool *dockertest.Pool) (string, func()) {
	if *postgresUrl != "" {
		return *postgresUrl, nil
	}

	resource, err := pool.Run("postgres", "11", []string{
		"POSTGRES_USER=parade",
		"POSTGRES_PASSWORD=parade",
		"POSTGRES_DB=parade_db",
	})
	if err != nil {
		log.Fatalf("could not start postgresql: %s", err)
	}

	// set cleanup
	closer := func() {
		err := pool.Purge(resource)
		if err != nil {
			log.Fatalf("could not kill postgres container")
		}
	}

	// expire, just to make sure
	err = resource.Expire(dbContainerTimeoutSeconds)
	if err != nil {
		log.Fatalf("could not expire postgres container")
	}

	// create connection
	var conn *sqlx.DB
	uri := fmt.Sprintf("postgres://parade:parade@localhost:%s/"+dbName+"?sslmode=disable", resource.GetPort("5432/tcp"))
	err = pool.Retry(func() error {
		var err error
		conn, err = sqlx.Connect("pgx", uri)
		if err != nil {
			return err
		}
		return conn.Ping()
	})
	if err != nil {
		log.Fatalf("could not connect to postgres: %s", err)
	}

	// Run the DDL
	if _, err = sqlx.LoadFile(conn, "./ddl.sql"); err != nil {
		log.Fatalf("exec command file ./ddl.sql: %s", err)
	}

	_ = conn.Close()

	// return DB URI
	return uri, closer
}

func TestMain(m *testing.M) {
	var err error
	flag.Parse()
	pool, err = dockertest.NewPool("")
	if err != nil {
		log.Fatalf("could not connect to Docker: %s", err)
	}
	var dbCleanup func()
	databaseURI, dbCleanup = runDBInstance(pool)
	defer dbCleanup() // In case we don't reach the cleanup action.
	db = sqlx.MustConnect("pgx", databaseURI)
	defer db.Close()
	code := m.Run()
	if _, ok := os.LookupEnv("GOTEST_KEEP_DB"); !ok && dbCleanup != nil {
		dbCleanup() // os.Exit() below won't call the defered cleanup, do it now.
	}
	os.Exit(code)
}

// wrapper derives a prefix from t.Name and uses it to provide namespaced access to  DB db as
// well as a simple error-reporting inserter.
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

func (w wrapper) prefixTask(id ddl.TaskId) ddl.TaskId {
	return ddl.TaskId(w.prefix(string(id)))
}

func (w wrapper) stripTask(id ddl.TaskId) ddl.TaskId {
	return ddl.TaskId(w.strip(string(id)))
}

func (w wrapper) prefixActor(actor ddl.ActorId) ddl.ActorId {
	return ddl.ActorId(w.prefix(string(actor)))
}

func (w wrapper) stripActor(actor ddl.TaskId) ddl.ActorId {
	return ddl.ActorId(w.strip(string(actor)))
}

func (w wrapper) insertTasks(tasks []ddl.TaskData) func() {
	w.t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, databaseURI)
	if err != nil {
		w.t.Fatalf("pgx.Connect: %s", err)
	}
	defer conn.Close(ctx)

	prefixedTasks := make([]ddl.TaskData, len(tasks))
	for i := 0; i < len(tasks); i++ {
		copy := tasks[i]
		copy.Id = w.prefixTask(copy.Id)
		copy.Action = w.prefix(copy.Action)
		copy.ActorId = w.prefixActor(copy.ActorId)
		if copy.StatusCode == "" {
			copy.StatusCode = "pending"
		}
		toSignal := make([]ddl.TaskId, len(copy.ToSignal))
		for j := 0; j < len(toSignal); j++ {
			toSignal[j] = w.prefixTask(copy.ToSignal[j])
		}
		copy.ToSignal = toSignal
		prefixedTasks[i] = copy
	}
	err = ddl.InsertTasks(ctx, conn, &ddl.TaskDataIterator{Data: prefixedTasks})
	if err != nil {
		w.t.Fatalf("InsertTasks: %s", err)
	}

	// Create cleanup callback.  Compute the ids now, tasks may change later.
	ids := make([]ddl.TaskId, 0, len(tasks))
	for _, task := range tasks {
		ids = append(ids, task.Id)
	}
	return func() { w.deleteTasks(ids) }
}

func (w wrapper) deleteTasks(ids []ddl.TaskId) error {
	prefixedIds := make([]ddl.TaskId, len(ids))
	for i := 0; i < len(ids); i++ {
		prefixedIds[i] = w.prefixTask(ids[i])
	}
	tx, err := w.db.BeginTxx(context.Background(), nil)
	if err != nil {
		return fmt.Errorf("BEGIN: %w", err)
	}
	defer func() {
		if tx != nil {
			tx.Rollback()
		}
	}()

	if err = ddl.DeleteTasks(tx, prefixedIds); err != nil {
		return err
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("COMMIT: %w", err)
	}
	tx = nil
	return nil
}

func (w wrapper) returnTask(taskId ddl.TaskId, token ddl.PerformanceToken, resultStatus string, resultStatusCode ddl.TaskStatusCodeValue) error {
	return ddl.ReturnTask(w.db, w.prefixTask(taskId), token, resultStatus, resultStatusCode)
}

func (w wrapper) ownTasks(actorId ddl.ActorId, maxTasks int, actions []string, maxDuration *time.Duration) ([]ddl.OwnedTaskData, error) {
	prefixedActions := make([]string, len(actions))
	for i, action := range actions {
		prefixedActions[i] = w.prefix(action)
	}
	tasks, err := ddl.OwnTasks(w.db, actorId, maxTasks, prefixedActions, maxDuration)
	if tasks != nil {
		for i := 0; i < len(tasks); i++ {
			task := &tasks[i]
			task.Id = w.stripTask(task.Id)
		}
	}
	return tasks, err
}

func stringAddr(s string) *string {
	return &s
}

func performanceTokenAddr(p ddl.PerformanceToken) *ddl.PerformanceToken {
	return &p
}

func intAddr(i int) *int {
	return &i
}

func TestTaskDataIterator_Empty(t *testing.T) {
	it := ddl.TaskDataIterator{Data: []ddl.TaskData{}}
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
	if !errors.Is(it.Err(), ddl.NoMoreDataError) {
		t.Errorf("expected twice-advanced iterator to raise NoMoreDataError, got %s", it.Err())
	}
}

func TestTaskDataIterator_Values(t *testing.T) {
	now := time.Now()
	tasks := []ddl.TaskData{
		{Id: "000", Action: "zero", StatusCode: "enum values enforced on DB"},
		{Id: "111", Action: "frob", Body: stringAddr("1"), Status: stringAddr("state"),
			StatusCode: "pending",
			NumTries:   11, MaxTries: intAddr(17),
			TotalDependencies: intAddr(9),
			ToSignal:          []ddl.TaskId{ddl.TaskId("foo"), ddl.TaskId("bar")},
			ActorId:           ddl.ActorId("actor"), ActionDeadline: &now,
			PerformanceToken:  performanceTokenAddr(ddl.PerformanceToken{}),
			FinishChannelName: stringAddr("done"),
		},
	}
	it := ddl.TaskDataIterator{Data: tasks}

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
				task.Id, task.Action, task.Body, task.Status, task.StatusCode,
				task.NumTries, task.MaxTries,
				task.TotalDependencies, toSignal,
				task.ActorId, task.ActionDeadline,
				task.PerformanceToken, task.FinishChannelName,
			}, values); diffs != nil {
			t.Errorf("got other values at index %d than expected: %s", index, diffs)
		}
	}
	if it.Next() {
		t.Errorf("expected iterator %+v to end after tasks done", it)
	}
	if _, err := it.Values(); !errors.Is(err, ddl.NoMoreDataError) {
		t.Errorf("expected NoMoreData after iterator done, got %s", err)
	}
	if !errors.Is(it.Err(), ddl.NoMoreDataError) {
		t.Errorf("expected iterator Err() to repeat NoMoreDataError, got %s", it.Err())
	}
	if it.Next() {
		t.Errorf("expected iterator %+v to end-and-error after advanced past tasks done", it)
	}
}

func TestOwn(t *testing.T) {
	w := wrapper{t, db}

	cleanup := w.insertTasks([]ddl.TaskData{
		{Id: "000", Action: "never"},
		{Id: "111", Action: "frob"},
		{Id: "123", Action: "broz"},
		{Id: "222", Action: "broz"},
	})
	defer cleanup()
	tasks, err := w.ownTasks(ddl.ActorId("tester"), 2, []string{"frob", "broz"}, nil)
	if err != nil {
		t.Errorf("first own_tasks query: %s", err)
	}
	if len(tasks) != 2 {
		t.Errorf("expected first OwnTasks to return 2 tasks but got %d: %+v", len(tasks), tasks)
	}
	gotTasks := tasks

	tasks, err = w.ownTasks(ddl.ActorId("tester-two"), 2, []string{"frob", "broz"}, nil)
	if err != nil {
		t.Errorf("second own_tasks query: %s", err)
	}
	if len(tasks) != 1 {
		t.Errorf("expected second OwnTasks to return 1 task but got %d: %+v", len(tasks), tasks)
	}
	gotTasks = append(gotTasks, tasks...)

	gotIds := make([]ddl.TaskId, 0, len(gotTasks))
	for _, got := range gotTasks {
		gotIds = append(gotIds, got.Id)
	}
	sort.Sort(taskIdSlice(gotIds))
	if diffs := deep.Equal([]ddl.TaskId{"111", "123", "222"}, gotIds); diffs != nil {
		t.Errorf("expected other task IDs: %s", diffs)
	}
}

func TestOwnBody(t *testing.T) {
	w := wrapper{t, db}

	val := "\"the quick brown fox jumps over the lazy dog\""

	cleanup := w.insertTasks([]ddl.TaskData{
		{Id: "body", Action: "yes", Body: &val},
		{Id: "nobody", Action: "no"},
	})
	defer cleanup()

	tasks, err := w.ownTasks(ddl.ActorId("somebody"), 2, []string{"yes", "no"}, nil)
	if err != nil {
		t.Fatalf("own tasks: %s", err)
	}
	if len(tasks) != 2 {
		t.Fatalf("expected to own 2 tasks but got %+v", tasks)
	}
	body, nobody := tasks[0], tasks[1]
	if body.Id != "body" {
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

	cleanup := w.insertTasks([]ddl.TaskData{
		{Id: "111", Action: "frob"},
	})
	defer cleanup()

	_, err := w.ownTasks(ddl.ActorId("tortoise"), 1, []string{"frob"}, &second)
	if err != nil {
		t.Fatalf("failed to setup tortoise task ownership: %s", err)
	}

	fastTasks, err := w.ownTasks(ddl.ActorId("hare"), 1, []string{"frob"}, &second)
	if err != nil {
		t.Fatalf("failed to request fast task ownership: %s", err)
	}
	if len(fastTasks) != 0 {
		t.Errorf("expected immedidate hare task ownership to return nothing but got %+v", fastTasks)
	}

	time.Sleep(2 * time.Second)
	fastTasks, err = w.ownTasks(ddl.ActorId("hare"), 1, []string{"frob"}, &second)
	if err != nil {
		t.Fatalf("failed to request fast task ownership after sleeping: %s", err)
	}
	if len(fastTasks) != 1 || fastTasks[0].Id != "111" {
		t.Errorf("expected eventual hare task ownership to return task \"111\" but got tasks %+v", fastTasks)
	}
}

func TestReturnTask_DirectlyAndRetry(t *testing.T) {
	w := wrapper{t, db}

	cleanup := w.insertTasks([]ddl.TaskData{
		{Id: "111", Action: "frob"},
		{Id: "123", Action: "broz"},
		{Id: "222", Action: "broz"},
	})
	defer cleanup()

	tasks, err := w.ownTasks(ddl.ActorId("foo"), 4, []string{"frob", "broz"}, nil)
	if err != nil {
		t.Fatalf("acquire all tasks: %s", err)
	}

	taskById := make(map[ddl.TaskId]*ddl.OwnedTaskData, len(tasks))
	for index := range tasks {
		taskById[tasks[index].Id] = &tasks[index]
	}

	if err = w.returnTask(taskById[ddl.TaskId("111")].Id, taskById[ddl.TaskId("111")].Token, "done", ddl.TASK_COMPLETED); err != nil {
		t.Errorf("return task 111: %s", err)
	}

	if err = w.returnTask(taskById[ddl.TaskId("111")].Id, taskById[ddl.TaskId("111")].Token, "done", ddl.TASK_COMPLETED); !errors.Is(err, ddl.InvalidTokenError) {
		t.Errorf("expected second attempt to return task 111 to fail with InvalidTokenError, got %s", err)
	}

	// Now attempt to return a task to in-progress state.
	if err = w.returnTask(taskById[ddl.TaskId("123")].Id, taskById[ddl.TaskId("123")].Token, "try-again", ddl.TASK_PENDING); err != nil {
		t.Errorf("return task 123 (%+v) for another round: %s", taskById[ddl.TaskId("123")], err)
	}
	moreTasks, err := w.ownTasks(ddl.ActorId("foo"), 4, []string{"frob", "broz"}, nil)
	if err != nil {
		t.Fatalf("re-acquire task 123: %s", err)
	}
	if len(moreTasks) != 1 || moreTasks[0].Id != ddl.TaskId("123") {
		t.Errorf("expected to receive only task 123 but got tasks %+v", moreTasks)
	}
}

func TestReturnTask_RetryMulti(t *testing.T) {
	w := wrapper{t, db}

	maxTries := 7
	lifetime := 250 * time.Millisecond

	cleanup := w.insertTasks([]ddl.TaskData{
		{Id: "111", Action: "frob", MaxTries: &maxTries},
	})
	defer cleanup()

	for i := 0; i < maxTries; i++ {
		tasks, err := w.ownTasks(ddl.ActorId("foo"), 1, []string{"frob"}, &lifetime)
		if err != nil {
			t.Errorf("acquire task after %d/%d tries: %s", i, maxTries, err)
		}
		if len(tasks) != 1 {
			t.Fatalf("expected to own single task after %d/%d tries but got %+v", i, maxTries, tasks)
		}
		if i%2 == 0 {
			time.Sleep(2 * lifetime)
		} else {
			if err = w.returnTask(tasks[0].Id, tasks[0].Token, "retry", ddl.TASK_PENDING); err != nil {
				t.Fatalf("return task %+v after %d/%d tries: %s", tasks[0], i, maxTries, err)
			}
		}
	}

	tasks, err := w.ownTasks(ddl.ActorId("foo"), 1, []string{"frob"}, &lifetime)
	if err != nil {
		t.Fatalf("re-acquire task failed: %s", err)
	}
	if len(tasks) != 0 {
		t.Errorf("expected not to receive any tasks (maxRetries)  but got tasks %+v", tasks)
	}
}

func TestDependencies(t *testing.T) {
	w := wrapper{t, db}

	id := func(n int) ddl.TaskId {
		return ddl.TaskId(fmt.Sprintf("number-%d", n))
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
	taskData := make([]ddl.TaskData, 0, num)
	for i := 1; i <= num; i++ {
		toSignal := make([]ddl.TaskId, 0, i/20)
		for j := 2 * i; j <= num; j += i {
			toSignal = append(toSignal, id(j))
		}
		numDivisors := 0
		for k := 1; k <= i/2; k++ {
			if i%k == 0 {
				numDivisors++
			}
		}

		taskData = append(taskData, ddl.TaskData{
			Id:                id(i),
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
		tasks, err := w.ownTasks(ddl.ActorId("foo"), 17, []string{"div"}, nil)
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
			if err = w.returnTask(task.Id, task.Token, "divided", ddl.TASK_COMPLETED); err != nil {
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

	cleanup := w.insertTasks([]ddl.TaskData{
		{Id: ddl.TaskId("a0"), Action: "root", ToSignal: []ddl.TaskId{"a1", "a3"}},
		{Id: ddl.TaskId("a1"), Action: "dep", ToSignal: []ddl.TaskId{"a2"}, TotalDependencies: intAddr(1)},
		{Id: ddl.TaskId("a2"), Action: "dep", ToSignal: []ddl.TaskId{"a3"}, TotalDependencies: intAddr(1)},
		{Id: ddl.TaskId("a3"), Action: "leaf", TotalDependencies: intAddr(2)},

		{Id: ddl.TaskId("b0"), Action: "root", ToSignal: []ddl.TaskId{"b1"}},
		{Id: ddl.TaskId("b1"), Action: "root-keep", ToSignal: []ddl.TaskId{"b2"}},
		{Id: ddl.TaskId("b2"), Action: "leaf", TotalDependencies: intAddr(2)},

		{Id: ddl.TaskId("c0"), Action: "root", ToSignal: []ddl.TaskId{"c1", "c2"}},
		{Id: ddl.TaskId("c1"), Action: "dep", ToSignal: []ddl.TaskId{"c3", "c4"}, TotalDependencies: intAddr(1)},
		{Id: ddl.TaskId("c2"), Action: "dep", ToSignal: []ddl.TaskId{"c4", "c5"}, TotalDependencies: intAddr(1)},
		{Id: ddl.TaskId("c3"), Action: "dep", ToSignal: []ddl.TaskId{"c5", "c6"}, TotalDependencies: intAddr(1)},
		{Id: ddl.TaskId("c4"), Action: "leaf", TotalDependencies: intAddr(2)},
		{Id: ddl.TaskId("c5"), Action: "leaf", TotalDependencies: intAddr(2)},
		{Id: ddl.TaskId("c6"), Action: "leaf", TotalDependencies: intAddr(1)},
	})
	defer cleanup()

	type testCase struct {
		title             string
		casePrefix        string
		toDelete          []ddl.TaskId
		expectedRemaining []ddl.TaskId
	}
	cases := []testCase{
		{title: "chain with extra link", casePrefix: "a", toDelete: []ddl.TaskId{"a0"}},
		{title: "delete only one dep", casePrefix: "b", toDelete: []ddl.TaskId{"b0"}, expectedRemaining: []ddl.TaskId{"b1", "b2"}},
		{title: "treelike", casePrefix: "c", toDelete: []ddl.TaskId{"c0"}},
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
			gotRemaining := make([]ddl.TaskId, 0, len(c.expectedRemaining))
			for rows.Next() {
				var id ddl.TaskId
				if err = rows.Scan(&id); err != nil {
					t.Errorf("[I] scan ID value: %s", err)
				}
				gotRemaining = append(gotRemaining, id)
			}
			sort.Sort(taskIdSlice(gotRemaining))
			expectedRemaining := c.expectedRemaining
			if expectedRemaining == nil {
				expectedRemaining = []ddl.TaskId{}
			}
			for i, e := range expectedRemaining {
				expectedRemaining[i] = w.prefixTask(e)
			}
			sort.Sort(taskIdSlice(expectedRemaining))
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
		id         ddl.TaskId
		status     string
		statusCode ddl.TaskStatusCodeValue
	}

	cases := []testCase{
		{"task aborted", ddl.TaskId("111"), "b0rked!", ddl.TASK_ABORTED},
		{"task succeeded", ddl.TaskId("222"), "yay!", ddl.TASK_COMPLETED},
	}

	for _, c := range cases {
		t.Run(c.title, func(t *testing.T) {
			cleanup := w.insertTasks([]ddl.TaskData{
				{Id: c.id, Action: "frob"},
			})
			defer cleanup()

			tasks, err := w.ownTasks(ddl.ActorId("foo"), 1, []string{"frob"}, nil)
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
				statusCode ddl.TaskStatusCodeValue
				err        error
			}
			ch := make(chan result)
			go func() {
				status, statusCode, err := ddl.WaitForTask(ctx, conn, ddl.TaskId("111"))
				ch <- result{status, statusCode, err}
			}()

			if err = w.returnTask(tasks[0].Id, tasks[0].Token, c.status, c.statusCode); err != nil {
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

	id := func(n int) ddl.TaskId {
		return ddl.TaskId(fmt.Sprintf("in:%08d", n))
	}

	tasks := make([]ddl.TaskData, 0, numTasks+1)
	toSignal := []ddl.TaskId{"done"}
	for i := 0; i < numTasks; i++ {
		tasks = append(tasks, ddl.TaskData{Id: id(i), Action: "part", ToSignal: toSignal})
	}
	tasks = append(tasks, ddl.TaskData{Id: "done", Action: "done"})
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
				size := *bulk + int(rand.Int31n(int32(*bulk/10))) - *bulk/10
				tasks, err := w.ownTasks(ddl.ActorId(fmt.Sprintf("worker-%d", i)), size, []string{"part", "done"}, nil)
				if err != nil {
					resultCh[i] <- result{err: err}
					return
				}
				if len(tasks) == 0 {
					break
				}
				for _, task := range tasks {
					if task.Id != "done" {
						count++
					} else {
						receivedDone = true
					}
					w.returnTask(task.Id, task.Token, "ok", ddl.TASK_COMPLETED)
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
