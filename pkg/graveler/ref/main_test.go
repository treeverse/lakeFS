package ref_test

import (
	"flag"
	"log"
	"os"
	"testing"

	"github.com/treeverse/lakefs/pkg/ident"

	"github.com/ory/dockertest/v3"
	"github.com/sirupsen/logrus"
	"github.com/treeverse/lakefs/pkg/db"
	"github.com/treeverse/lakefs/pkg/graveler/ref"
	"github.com/treeverse/lakefs/pkg/testutil"
)

var (
	pool        *dockertest.Pool
	databaseURI string
)

func testRefManager(t testing.TB) *ref.Manager {
	t.Helper()
	conn, _ := testutil.GetDB(t, databaseURI, testutil.WithGetDBApplyDDL(true))
	return ref.NewPGRefManager(conn, ident.NewHexAddressProvider())
}

func testRefManagerWithDB(t testing.TB) (*ref.Manager, db.Database) {
	t.Helper()
	conn, _ := testutil.GetDB(t, databaseURI, testutil.WithGetDBApplyDDL(true))
	return ref.NewPGRefManager(conn, ident.NewHexAddressProvider()), conn
}

func testRefManagerWithAddressProvider(t testing.TB, addressProvider ident.AddressProvider) *ref.Manager {
	t.Helper()
	conn, _ := testutil.GetDB(t, databaseURI, testutil.WithGetDBApplyDDL(true))
	return ref.NewPGRefManager(conn, addressProvider)
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
