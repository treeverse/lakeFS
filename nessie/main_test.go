package nessie

import (
	"flag"
	"log"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go/service/s3"
	genclient "github.com/treeverse/lakefs/pkg/api/gen/client"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/testutil"
)

var (
	logger logging.Logger
	client *genclient.Lakefs
	svc    *s3.S3
	server *webhookServer
)

func TestMain(m *testing.M) {
	systemTests := flag.Bool("system-tests", false, "Run system tests")
	useLocalCredentials := flag.Bool("use-local-credentials", false, "Generate local API key during `lakefs setup'")

	flag.Parse()
	if !*systemTests {
		os.Exit(0)
	}

	params := testutil.SetupTestingEnvParams{
		Name:      "nessie",
		StorageNS: "nessie-system-testing",
	}
	if *useLocalCredentials {
		params.AdminAccessKeyID = "AKIAIOSFODNN7EXAMPLE"
		params.AdminSecretAccessKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	}
	logger, client, svc = testutil.SetupTestingEnv(&params)
	var err error
	server, err = startWebhookServer()
	if err != nil {
		log.Fatal(err)
	}

	defer func() { _ = server.s.Close() }()

	logger.Info("Setup succeeded, running the tests")
	os.Exit(m.Run())
}
