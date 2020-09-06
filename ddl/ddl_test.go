package ddl_test

import (
	"errors"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/go-test/deep"
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
)

// taskIdSlice attaches the methods of sort.Interface to []TaskId.
type taskIdSlice []ddl.TaskId

func (p taskIdSlice) Len() int           { return len(p) }
func (p taskIdSlice) Less(i, j int) bool { return p[i] < p[j] }
func (p taskIdSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

// runDBInstance starts a test Postgres server inside container pool, and returns a connection
// URI and a closer function.
func runDBInstance(pool *dockertest.Pool) (string, func()) {
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
	pool, err = dockertest.NewPool("")
	if err != nil {
		log.Fatalf("could not connect to Docker: %s", err)
	}
	databaseURI, dbCleanup := runDBInstance(pool)
	defer dbCleanup() // In case we don't reach the cleanup action.
	db = sqlx.MustConnect("pgx", databaseURI)
	code := m.Run()
	if _, ok := os.LookupEnv("GOTEST_KEEP_DB"); !ok {
		dbCleanup() // os.Exit() below won't call the defered cleanup, do it now.
	}
	os.Exit(code)
}

// wrapper derives a prefix from t.Name and uses it to provide namespaced access to  DB db as
// well as a simple error-reporting inserter.
type wrapper struct {
	t  *testing.T
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

func (w wrapper) insertTasks(tasks []ddl.TaskData) {
	w.t.Helper()
	const insertSql = `INSERT INTO tasks
		(id, action, body, status, status_code, num_tries, max_tries, actor_id,
		 action_deadline, performance_token, finish_channel)
		VALUES(:task_id, :action, :body, :status, :status_code, :num_tries, :max_tries, :actor_id,
		       :action_deadline, :performance_token, :finish_channel)`
	for _, task := range tasks {
		copy := task
		copy.Id = w.prefixTask(copy.Id)
		copy.Action = w.prefix(copy.Action)
		copy.ActorId = w.prefixActor(copy.ActorId)
		if copy.StatusCode == "" {
			copy.StatusCode = "pending"
		}
		_, err := w.db.NamedExec(insertSql, copy)
		if err != nil {
			w.t.Fatalf("insert %+v into tasks: %s", tasks, err)
		}
	}
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

func TestOwn(t *testing.T) {
	w := wrapper{t, db}

	w.insertTasks([]ddl.TaskData{
		{Id: "000", Action: "never"},
		{Id: "111", Action: "frob"},
		{Id: "123", Action: "broz"},
		{Id: "222", Action: "broz"},
	})
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

func TestOwnAfterDeadlineElapsed(t *testing.T) {
	second := 1 * time.Second
	w := wrapper{t, db}

	w.insertTasks([]ddl.TaskData{
		{Id: "111", Action: "frob"},
	})
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

	w.insertTasks([]ddl.TaskData{
		{Id: "111", Action: "frob"},
		{Id: "123", Action: "broz"},
		{Id: "222", Action: "broz"},
	})

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

	w.insertTasks([]ddl.TaskData{
		{Id: "111", Action: "frob", MaxTries: &maxTries},
	})

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
