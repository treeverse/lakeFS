package migrate

import (
	"flag"
	"github.com/treeverse/lakefs/pkg/db"
	"github.com/treeverse/lakefs/pkg/db/params"
	"log"
	"os"
	"testing"

	"github.com/ory/dockertest/v3"
	"github.com/sirupsen/logrus"
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

	err = db.MigrateUp(params.Database{
		ConnectionString: databaseURI,
	})

	if err != nil {
		log.Fatalf("Could not migrate DB: %s", err)
	}
	code := m.Run()
	closer() // cleanup
	os.Exit(code)
}
