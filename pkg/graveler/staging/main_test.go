package staging_test

import (
	"flag"
	"os"
	"testing"

	"github.com/ory/dockertest/v3"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/testutil"
)

var (
	pool        *dockertest.Pool
	databaseURI string
)

func TestMain(m *testing.M) {
	flag.Parse()
	if !testing.Verbose() {
		// keep the log level calm
		logging.SetLevel("panic")
	}

	// postgres container
	var err error
	pool, err = dockertest.NewPool("")
	if err != nil {
		logging.Default().Fatalf("Could not connect to Docker: %s", err)
	}
	var closer func()
	databaseURI, closer = testutil.GetDBInstance(pool)

	code := m.Run()
	closer() // cleanup
	os.Exit(code)
}
