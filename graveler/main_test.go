package graveler_test

import (
	"flag"
	"log"
	"os"
	"testing"

	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/graveler"
	"github.com/treeverse/lakefs/testutil"

	"github.com/ory/dockertest/v3"
	"github.com/sirupsen/logrus"
)

var (
	pool        *dockertest.Pool
	databaseURI string
)

func testRefManager(t testing.TB) graveler.RefManager {
	t.Helper()
	conn, _ := testutil.GetDB(t, databaseURI, testutil.WithGetDBApplyDDL(true))
	return graveler.NewPGRefManager(conn)
}

func testRefManagerWithDB(t testing.TB) (graveler.RefManager, db.Database) {
	t.Helper()
	conn, _ := testutil.GetDB(t, databaseURI, testutil.WithGetDBApplyDDL(true))
	return graveler.NewPGRefManager(conn), conn
}

func TestMain(m *testing.M) {
	flag.Parse()
	if !testing.Verbose() {
		// keep the log level calm
		logrus.SetLevel(logrus.PanicLevel)
	}

	// postgres container
	var err error
	pool, err = dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to Docker: %s", err)
	}
	var closer func()
	databaseURI, closer = testutil.GetDBInstance(pool)
	code := m.Run()
	closer() // cleanup
	os.Exit(code)
}
