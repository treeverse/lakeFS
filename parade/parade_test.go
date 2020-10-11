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
	"testing"
	"time"

	"github.com/go-test/deep"
	"github.com/jackc/pgtype"
	"github.com/treeverse/lakefs/parade"
	"github.com/treeverse/lakefs/testutil"

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
	numShards   = flag.Int("num-shards", 400, "Number of intermediate fan-in shards")
)

// taskIDSlice attaches the methods of sort.Interface to []TaskID.
type taskIDSlice []parade.TaskID

func (p taskIDSlice) Len() int           { return len(p) }
func (p taskIDSlice) Less(i, j int) bool { return p[i] < p[j] }
func (p taskIDSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

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

func stringAddr(s string) *string {
	return &s
}

func performanceTokenAddr(p parade.PerformanceToken) *parade.PerformanceToken {
	return &p
}

func intAddr(i int) *int {
	return &i
}

func scanIDs(t *testing.T, prefix string) []parade.TaskID {
	t.Helper()
	rows, err := db.Query(`SELECT id FROM tasks WHERE id LIKE format('%s%%', $1::text)`, prefix)
	if err != nil {
		t.Errorf("[I] select remaining IDs for prefix %s: %s", prefix, err)
	}

	defer func() {
		if err := rows.Close(); err != nil {
			t.Fatalf("[I] remaining ids iterator close: %s", err)
		}
	}()
	gotIDs := make([]parade.TaskID, 0)
	for rows.Next() {
		var id parade.TaskID
		if err = rows.Scan(&id); err != nil {
			t.Errorf("[I] scan ID value: %s", err)
		}
		gotIDs = append(gotIDs, id)
	}
	return gotIDs
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
			NumSignals: 7, TotalDependencies: intAddr(9),
			ToSignalAfter: []parade.TaskID{parade.TaskID("foo"), parade.TaskID("bar")},
			ActorID:       parade.ActorID("actor"), ActionDeadline: &now,
			PerformanceToken:   performanceTokenAddr(parade.PerformanceToken{}),
			NotifyChannelAfter: stringAddr("done"),
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
		toSignalAfter := make([]string, len(task.ToSignalAfter))
		for i := 0; i < len(task.ToSignalAfter); i++ {
			toSignalAfter[i] = string(task.ToSignalAfter[i])
		}
		if diffs := deep.Equal(
			[]interface{}{
				task.ID, task.Action, task.Body, task.Status, task.StatusCode,
				task.NumTries, task.MaxTries,
				task.NumSignals, task.TotalDependencies,
				task.ActorID, task.ActionDeadline,
				task.PerformanceToken, toSignalAfter, task.NotifyChannelAfter,
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

func TestTaskStatusCodeValueScan(t *testing.T) {
	cases := []struct {
		name string
		in   interface{}
		err  error
		out  parade.TaskStatusCodeValue
	}{
		{"emptyString", "", nil, parade.TaskInvalid},
		{"int", -17, parade.ErrBadTypeConversion, ""},
		{"nil", nil, parade.ErrBadTypeConversion, ""},
		{"pending", "pending", nil, parade.TaskPending},
		{"PENdiNG", "PENdiNG", nil, parade.TaskPending},
		{"in-progress", "in-progress", nil, parade.TaskInProgress},
		{"IN-ProgRESS", "IN-ProgRESS", nil, parade.TaskInProgress},
		{"completed", "completed", nil, parade.TaskCompleted},
		{"COMPLETED", "COMPLETED", nil, parade.TaskCompleted},
		{"invalid", "huh?", nil, parade.TaskInvalid},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var dst parade.TaskStatusCodeValue
			if err := dst.Scan(c.in); err != nil {
				if !errors.Is(err, c.err) {
					t.Errorf("got err %v, expected %v", err, c.err)
				}
			} else {
				if dst != c.out {
					t.Errorf("expected %s, got %s", c.out, dst)
				}
			}
		})
	}
}

func makeParadePrefix(t testing.TB) *parade.ParadePrefix {
	return &parade.ParadePrefix{parade.NewParadeDB(db), t.Name()}
}

// makeCleanup returns a cleanup for tasks that you can defer, that ignores any changes to tasks
// that may occur after its call.
func makeCleanup(t testing.TB, ctx context.Context, pp *parade.ParadePrefix, tasks []parade.TaskData) func() {
	ids := make([]parade.TaskID, 0, len(tasks))

	for _, task := range tasks {
		ids = append(ids, task.ID)
	}

	return func() { testutil.MustDo(t, "cleanup DeleteTasks", pp.DeleteTasks(ctx, ids)) }
}

func TestOwn(t *testing.T) {
	ctx := context.Background()
	pp := makeParadePrefix(t)

	tasks := []parade.TaskData{
		{ID: "000", Action: "never"},
		{ID: "111", Action: "frob"},
		{ID: "123", Action: "broz"},
		{ID: "222", Action: "broz"},
	}
	testutil.MustDo(t, "InsertTasks", pp.InsertTasks(ctx, tasks))
	defer makeCleanup(t, ctx, pp, tasks)()
	ownedTasks, err := pp.OwnTasks(parade.ActorID("tester"), 2, []string{"frob", "broz"}, nil)
	if err != nil {
		t.Errorf("first own_tasks query: %s", err)
	}
	if len(ownedTasks) != 2 {
		t.Errorf("expected first OwnTasks to return 2 tasks but got %d: %+v", len(ownedTasks), ownedTasks)
	}
	gotTasks := ownedTasks

	ownedTasks, err = pp.OwnTasks(parade.ActorID("tester-two"), 2, []string{"frob", "broz"}, nil)
	if err != nil {
		t.Errorf("second own_tasks query: %s", err)
	}
	if len(ownedTasks) != 1 {
		t.Errorf("expected second OwnTasks to return 1 task but got %d: %+v", len(ownedTasks), ownedTasks)
	}
	gotTasks = append(gotTasks, ownedTasks...)

	gotIDs := make([]parade.TaskID, 0, len(gotTasks))
	for _, got := range gotTasks {
		gotIDs = append(gotIDs, got.ID)
	}
	sort.Sort(taskIDSlice(gotIDs))
	if diffs := deep.Equal([]parade.TaskID{"111", "123", "222"}, gotIDs); diffs != nil {
		t.Errorf("expected other task IDs: %s", diffs)
	}
}

func TestOwnBody(t *testing.T) {
	ctx := context.Background()
	pp := makeParadePrefix(t)

	val := "\"the quick brown fox jumps over the lazy dog\""

	tasks := []parade.TaskData{
		{ID: "body", Action: "yes", Body: &val},
		{ID: "nobody", Action: "no"},
	}
	testutil.MustDo(t, "InsertTasks", pp.InsertTasks(ctx, tasks))
	defer makeCleanup(t, ctx, pp, tasks)()

	ownedTasks, err := pp.OwnTasks(parade.ActorID("somebody"), 2, []string{"yes", "no"}, nil)
	if err != nil {
		t.Fatalf("own tasks: %s", err)
	}
	if len(ownedTasks) != 2 {
		t.Fatalf("expected to own 2 tasks but got %+v", ownedTasks)
	}
	body, nobody := ownedTasks[0], ownedTasks[1]
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
	ctx := context.Background()
	pp := makeParadePrefix(t)

	second := 1 * time.Second

	tasks := []parade.TaskData{
		{ID: "111", Action: "frob"},
	}
	testutil.MustDo(t, "InsertTasks", pp.InsertTasks(ctx, tasks))
	defer makeCleanup(t, ctx, pp, tasks)()

	_, err := pp.OwnTasks(parade.ActorID("tortoise"), 1, []string{"frob"}, &second)
	if err != nil {
		t.Fatalf("failed to setup tortoise task ownership: %s", err)
	}

	fastTasks, err := pp.OwnTasks(parade.ActorID("hare"), 1, []string{"frob"}, &second)
	if err != nil {
		t.Fatalf("failed to request fast task ownership: %s", err)
	}
	if len(fastTasks) != 0 {
		t.Errorf("expected immedidate hare task ownership to return nothing but got %+v", fastTasks)
	}

	time.Sleep(2 * time.Second)
	fastTasks, err = pp.OwnTasks(parade.ActorID("hare"), 1, []string{"frob"}, &second)
	if err != nil {
		t.Fatalf("failed to request fast task ownership after sleeping: %s", err)
	}
	if len(fastTasks) != 1 || fastTasks[0].ID != "111" {
		t.Errorf("expected eventual hare task ownership to return task \"111\" but got tasks %+v", fastTasks)
	}
}

func TestReturnTask_DirectlyAndRetry(t *testing.T) {
	ctx := context.Background()
	pp := makeParadePrefix(t)

	tasks := []parade.TaskData{
		{ID: "111", Action: "frob"},
		{ID: "123", Action: "broz"},
		{ID: "222", Action: "broz"},
	}
	testutil.MustDo(t, "InsertTasks", pp.InsertTasks(ctx, tasks))
	defer makeCleanup(t, ctx, pp, tasks)()

	ownedTasks, err := pp.OwnTasks(parade.ActorID("foo"), 4, []string{"frob", "broz"}, nil)
	if err != nil {
		t.Fatalf("acquire all tasks: %s", err)
	}

	taskByID := make(map[parade.TaskID]*parade.OwnedTaskData, len(ownedTasks))
	for index := range ownedTasks {
		taskByID[ownedTasks[index].ID] = &ownedTasks[index]
	}

	if err = pp.ReturnTask(taskByID[parade.TaskID("111")].ID, taskByID[parade.TaskID("111")].Token, "done", parade.TaskCompleted); err != nil {
		t.Errorf("return task 111: %s", err)
	}

	if err = pp.ReturnTask(taskByID[parade.TaskID("111")].ID, taskByID[parade.TaskID("111")].Token, "done", parade.TaskCompleted); !errors.Is(err, parade.ErrInvalidToken) {
		t.Errorf("expected second attempt to return task 111 to fail with InvalidTokenError, got %s", err)
	}

	// Now attempt to return a task to in-progress state.
	if err = pp.ReturnTask(taskByID[parade.TaskID("123")].ID, taskByID[parade.TaskID("123")].Token, "try-again", parade.TaskPending); err != nil {
		t.Errorf("return task 123 (%+v) for another round: %s", taskByID[parade.TaskID("123")], err)
	}
	moreTasks, err := pp.OwnTasks(parade.ActorID("foo"), 4, []string{"frob", "broz"}, nil)
	if err != nil {
		t.Fatalf("re-acquire task 123: %s", err)
	}
	if len(moreTasks) != 1 || moreTasks[0].ID != parade.TaskID("123") {
		t.Errorf("expected to receive only task 123 but got tasks %+v", moreTasks)
	}
}

func TestReturnTask_RetryMulti(t *testing.T) {
	ctx := context.Background()
	pp := makeParadePrefix(t)

	maxTries := 7
	lifetime := 250 * time.Millisecond

	tasks := []parade.TaskData{
		{ID: "111", Action: "frob", MaxTries: &maxTries},
	}
	testutil.MustDo(t, "InsertTasks", pp.InsertTasks(ctx, tasks))
	defer makeCleanup(t, ctx, pp, tasks)()

	for i := 0; i < maxTries; i++ {
		ownedTasks, err := pp.OwnTasks(parade.ActorID("foo"), 1, []string{"frob"}, &lifetime)
		if err != nil {
			t.Errorf("acquire task after %d/%d tries: %s", i, maxTries, err)
		}
		if len(ownedTasks) != 1 {
			t.Fatalf("expected to own single task after %d/%d tries but got %+v", i, maxTries, ownedTasks)
		}
		if i%2 == 0 {
			time.Sleep(2 * lifetime)
		} else {
			if err = pp.ReturnTask(ownedTasks[0].ID, ownedTasks[0].Token, "retry", parade.TaskPending); err != nil {
				t.Fatalf("return task %+v after %d/%d tries: %s", ownedTasks[0], i, maxTries, err)
			}
		}
	}

	ownedTasks, err := pp.OwnTasks(parade.ActorID("foo"), 1, []string{"frob"}, &lifetime)
	if err != nil {
		t.Fatalf("re-acquire task failed: %s", err)
	}
	if len(ownedTasks) != 0 {
		t.Errorf("expected not to receive any tasks (maxRetries)  but got tasks %+v", tasks)
	}
}

func TestExtendTaskDuration(t *testing.T) {
	ctx := context.Background()
	pp := makeParadePrefix(t)

	tasks := []parade.TaskData{
		{ID: "111", Action: "frob"},
		{ID: "222", Action: "broz"},
	}

	testutil.MustDo(t, "InsertTasks", pp.InsertTasks(ctx, tasks))
	defer makeCleanup(t, ctx, pp, tasks)()

	var invalidUUID pgtype.UUID
	if err := invalidUUID.DecodeText(nil, []byte("123e4567-e89b-12d3-a456-426614174000")); err != nil {
		t.Fatalf("generate fake performance token: %s", err)
	}
	invalidToken := parade.PerformanceToken(invalidUUID)

	shortLifetime := 100 * time.Millisecond
	longLifetime := time.Second

	t.Run("ExtendUnowned", func(t *testing.T) {
		if err := pp.ExtendTaskDeadline("111", invalidToken, time.Hour); !errors.Is(err, parade.ErrInvalidToken) {
			t.Errorf("expected to fail with invalid token, got %s", err)
		}
	})
	t.Run("ExtendOwned", func(t *testing.T) {
		ownedTasks, err := pp.OwnTasks(parade.ActorID("extend"), 1, []string{"frob"}, &shortLifetime)
		if err != nil {
			t.Fatalf("failed to own task 111 to frob: %s", err)
		}
		if len(ownedTasks) != 1 {
			t.Fatalf("expected to own single task, got %v", ownedTasks)
		}
		err = pp.ExtendTaskDeadline(ownedTasks[0].ID, ownedTasks[0].Token, longLifetime)
		if err != nil {
			t.Errorf("couldn't extend task ownership of %v: %s", ownedTasks[0], err)
		}
		time.Sleep(3 * shortLifetime)
		moreOwnedTasks, err := pp.OwnTasks(parade.ActorID("steal"), 1, []string{"frob"}, nil)
		if err != nil {
			t.Fatalf("failed to try to own tasks: %s", err)
		}
		if len(moreOwnedTasks) != 0 {
			t.Errorf("expected task 111 to still be owned, managed to own %v", moreOwnedTasks)
		}
	})
	t.Run("ExtendFailsWhenAnotherActorSteals", func(t *testing.T) {
		ownedTasks, err := pp.OwnTasks(parade.ActorID("first"), 1, []string{"broz"}, &shortLifetime)
		if err != nil {
			t.Fatalf("failed to own task 222 to broz: %s", err)
		}
		if len(ownedTasks) != 1 {
			t.Fatalf("expected to own single task, got %v", ownedTasks)
		}

		// Wait for it to expire, grab the task by another actor
		var moreOwnedTasks []parade.OwnedTaskData
		for i := 0; i < 5 && len(moreOwnedTasks) == 0; i++ {
			time.Sleep(shortLifetime)
			moreOwnedTasks, err = pp.OwnTasks(parade.ActorID("second"), 1, []string{"broz"}, &shortLifetime)
			if err != nil {
				t.Fatalf("failed to re-own task 222 to broz: %s", err)
			}
		}
		if len(moreOwnedTasks) != 1 {
			t.Fatalf("task 222 never expired, got just %v", moreOwnedTasks)
		}

		err = pp.ExtendTaskDeadline(ownedTasks[0].ID, ownedTasks[0].Token, shortLifetime)
		if !errors.Is(err, parade.ErrInvalidToken) {
			t.Fatalf("expected ownership of %v to have expired, got %s", ownedTasks[0], err)
		}
	})
}

func TestDependencies(t *testing.T) {
	ctx := context.Background()
	pp := makeParadePrefix(t)

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
		toSignalAfter := make([]parade.TaskID, 0, i/20)
		for j := 2 * i; j <= num; j += i {
			toSignalAfter = append(toSignalAfter, id(j))
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
			ToSignalAfter:     toSignalAfter,
			TotalDependencies: &numDivisors,
		})
	}

	testutil.MustDo(t, "InsertTasks", pp.InsertTasks(ctx, taskData))
	defer makeCleanup(t, ctx, pp, taskData)()

	doneSet := make(map[int]struct{}, num)

	for {
		ownedTasks, err := pp.OwnTasks(parade.ActorID("foo"), 17, []string{"div"}, nil)
		if err != nil {
			t.Fatalf("acquire tasks with done %+v: %s", doneSet, err)
		}
		if len(ownedTasks) == 0 {
			break
		}
		for _, task := range ownedTasks {
			n := parseBody(*task.Body)
			doneSet[n] = struct{}{}
			for d := 1; d <= n/2; d++ {
				if n%d == 0 {
					if _, ok := doneSet[d]; !ok {
						t.Errorf("retrieved task %+v before task for its divisor %d", task, d)
					}
				}
			}
			if err = pp.ReturnTask(task.ID, task.Token, "divided", parade.TaskCompleted); err != nil {
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
	ctx := context.Background()
	pp := makeParadePrefix(t)

	tasks := []parade.TaskData{
		{ID: parade.TaskID("a0"), Action: "root", ToSignalAfter: []parade.TaskID{"a1", "a3"}},
		{ID: parade.TaskID("a1"), Action: "dep", ToSignalAfter: []parade.TaskID{"a2"}, TotalDependencies: intAddr(1)},
		{ID: parade.TaskID("a2"), Action: "dep", ToSignalAfter: []parade.TaskID{"a3"}, NumSignals: 1, TotalDependencies: intAddr(2)},
		{ID: parade.TaskID("a3"), Action: "leaf", TotalDependencies: intAddr(2)},

		{ID: parade.TaskID("b0"), Action: "root", ToSignalAfter: []parade.TaskID{"b2"}},
		{ID: parade.TaskID("b1"), Action: "root-keep", ToSignalAfter: []parade.TaskID{"b2"}},
		{ID: parade.TaskID("b2"), Action: "leaf", TotalDependencies: intAddr(2)},

		{ID: parade.TaskID("c0"), Action: "root", ToSignalAfter: []parade.TaskID{"c1", "c2"}},
		{ID: parade.TaskID("c1"), Action: "dep", ToSignalAfter: []parade.TaskID{"c3", "c4"}, TotalDependencies: intAddr(1)},
		{ID: parade.TaskID("c2"), Action: "dep", ToSignalAfter: []parade.TaskID{"c4", "c5"}, TotalDependencies: intAddr(1)},
		{ID: parade.TaskID("c3"), Action: "dep", ToSignalAfter: []parade.TaskID{"c5", "c6"}, TotalDependencies: intAddr(1)},
		{ID: parade.TaskID("c4"), Action: "leaf", TotalDependencies: intAddr(2)},
		{ID: parade.TaskID("c5"), Action: "leaf", TotalDependencies: intAddr(2)},
		{ID: parade.TaskID("c6"), Action: "leaf", TotalDependencies: intAddr(1)},

		{ID: parade.TaskID("d0"), Action: "done", StatusCode: parade.TaskCompleted, ToSignalAfter: []parade.TaskID{"d1"}},
		{ID: parade.TaskID("d1"), Action: "new", NumSignals: 1, TotalDependencies: intAddr(2)},

		{ID: parade.TaskID("e0"), Action: "done", ToSignalAfter: []parade.TaskID{"e1"}},
		{ID: parade.TaskID("e1"), Action: "new", NumSignals: 1, TotalDependencies: intAddr(2)},
	}

	testutil.MustDo(t, "InsertTasks", pp.InsertTasks(ctx, tasks))
	defer makeCleanup(t, ctx, pp, tasks)()

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
		{title: "delete done", casePrefix: "d", toDelete: []parade.TaskID{"d0"}, expectedRemaining: []parade.TaskID{"d1"}},
		{title: "delete in-progress", casePrefix: "e", toDelete: []parade.TaskID{"e0"}},
	}
	prefix := t.Name()
	for _, c := range cases {
		t.Run(c.title, func(t *testing.T) {
			casePrefix := fmt.Sprint(prefix, ".", c.casePrefix)
			if err := pp.DeleteTasks(ctx, c.toDelete); err != nil {
				t.Errorf("DeleteTasks failed: %s", err)
			}

			gotRemaining := scanIDs(t, casePrefix)
			sort.Sort(taskIDSlice(gotRemaining))
			expectedRemaining := c.expectedRemaining
			if expectedRemaining == nil {
				expectedRemaining = []parade.TaskID{}
			}
			for i, e := range expectedRemaining {
				expectedRemaining[i] = pp.AddPrefixTask(e)
			}
			sort.Sort(taskIDSlice(expectedRemaining))
			if diffs := deep.Equal(expectedRemaining, gotRemaining); diffs != nil {
				t.Errorf("left with other IDs than expected: %s", diffs)
			}
		})
	}
}

func TestNotification(t *testing.T) {
	type testCase struct {
		title      string
		id         parade.TaskID
		status     string
		statusCode parade.TaskStatusCodeValue
	}

	cases := []testCase{
		{"task aborted", parade.TaskID("111"), "b0rked", parade.TaskAborted},
		{"task succeeded", parade.TaskID("222"), "yay", parade.TaskCompleted},
	}

	for _, c := range cases {
		runCase := func(t *testing.T, listenFirst bool) {
			ctx := context.Background()
			pp := makeParadePrefix(t)
			tasks := []parade.TaskData{
				{ID: c.id, Action: "frob", StatusCode: "pending", Body: stringAddr(""), NotifyChannelAfter: stringAddr(pp.AddPrefix("done"))},
			}

			testutil.MustDo(t, "InsertTasks", pp.InsertTasks(ctx, tasks))
			defer makeCleanup(t, ctx, pp, tasks)()

			ownedTasks, err := pp.OwnTasks(parade.ActorID("foo"), 1, []string{"frob"}, nil)
			if err != nil {
				t.Fatalf("acquire task: %s", err)
			}
			if len(ownedTasks) != 1 {
				t.Fatalf("expected to own single task but got %+v", ownedTasks)
			}
			task := ownedTasks[0]

			var waiter parade.Waiter

			if listenFirst {
				waiter, err = pp.NewWaiter(ctx, task.ID)
				if err != nil {
					t.Fatalf("Start waiting for %+v: %s", task, err)
				}
			}

			if err = pp.ReturnTask(task.ID, task.Token, c.status, c.statusCode); err != nil {
				t.Fatalf("return task %+v: %s", task, err)
			}

			if !listenFirst {
				waiter, err = pp.NewWaiter(ctx, task.ID)
				if err != nil {
					t.Fatalf("Start waiting for %+v: %s", task, err)
				}
			}

			status, statusCode, err := waiter.Wait()

			if err != nil {
				t.Errorf("wait failed: %s", err)
			}
			if c.status != status {
				t.Errorf("expected status %s but got %s", c.status, status)
			}
			if c.statusCode != statusCode {
				t.Errorf("expected status code %s but got %s", c.statusCode, statusCode)
			}
		}
		t.Run(c.title+" (return before wait)", func(t *testing.T) { runCase(t, false) })
		t.Run(c.title+" (wait before return)", func(t *testing.T) { runCase(t, true) })
	}
}

func BenchmarkFanIn(b *testing.B) {
	ctx := context.Background()
	pp := makeParadePrefix(b)

	numTasks := b.N * *taskFactor

	id := func(n int) parade.TaskID {
		return parade.TaskID(fmt.Sprintf("in:%08d", n))
	}
	shardID := func(n int) parade.TaskID {
		return parade.TaskID(fmt.Sprintf("done:%05d", n))
	}

	tasks := make([]parade.TaskData, 0, numTasks+*numShards+1)
	totalShardDependencies := make([]int, *numShards)
	for i := 0; i < numTasks; i++ {
		shard := (i / 100) % *numShards
		toSignalAfter := []parade.TaskID{shardID(shard)}
		tasks = append(tasks, parade.TaskData{ID: id(i), Action: "part", ToSignalAfter: toSignalAfter})
		totalShardDependencies[shard]++
	}

	toSignalAfter := []parade.TaskID{"done"}
	for i := 0; i < *numShards; i++ {
		tasks = append(tasks, parade.TaskData{ID: shardID(i), Action: "spontaneous", ToSignalAfter: toSignalAfter, TotalDependencies: &totalShardDependencies[i]})
	}
	tasks = append(tasks, parade.TaskData{ID: "done", Action: "done", TotalDependencies: numShards})

	testutil.MustDo(b, "InsertTasks", pp.InsertTasks(ctx, tasks))
	defer makeCleanup(b, ctx, pp, tasks)()

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
				tasks, err := pp.OwnTasks(
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
					case pp.AddPrefix("part"):
						count++
					case pp.AddPrefix("spontaneous"):
						// nothing, just reduce fan-in contention
					case pp.AddPrefix("done"):
						receivedDone = true
					default:
						resultCh[i] <- result{err: fmt.Errorf("weird action %s", task.Action)}
					}
					pp.ReturnTask(task.ID, task.Token, "ok", parade.TaskCompleted)
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
