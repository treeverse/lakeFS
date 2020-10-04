package nessie

import (
	"flag"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go/service/s3"
	genclient "github.com/treeverse/lakefs/api/gen/client"
	"github.com/treeverse/lakefs/logging"
	"github.com/treeverse/lakefs/testutil"
)

var (
	logger logging.Logger
	client *genclient.Lakefs
	svc    *s3.S3
)

func TestMain(m *testing.M) {
	systemTests := flag.Bool("system-tests", false, "Run system tests")
	flag.Parse()
	if !*systemTests {
		os.Exit(0)
	}

	logger, client, svc = testutil.SetupTestingEnv("nessie", "nessie-system-testing")
	logger.Info("Setup succeeded, running the tests")

	os.Exit(m.Run())
}