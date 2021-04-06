package nessie

import (
	"flag"
	"log"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/testutil"
)

type Booleans []bool

func (bs *Booleans) String() string {
	ret := make([]string, len(*bs))
	for i, b := range *bs {
		if b {
			ret[i] = "true"
		} else {
			ret[i] = "false"
		}
	}
	return strings.Join(ret, ",")
}

func (bs *Booleans) Parse(value string) error {
	values := strings.Split(value, ",")
	*bs = make(Booleans, 0, len(values))
	for _, v := range values {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return err
		}
		*bs = append(*bs, b)
	}
	return nil
}

var (
	logger logging.Logger
	client api.ClientWithResponsesInterface
	svc    *s3.S3
	server *webhookServer

	testDirectDataAccess = Booleans{false}
)

func TestMain(m *testing.M) {
	systemTests := flag.Bool("system-tests", false, "Run system tests")
	useLocalCredentials := flag.Bool("use-local-credentials", false, "Generate local API key during `lakefs setup'")

	if directs, ok := os.LookupEnv("NESSIE_TEST_DATA_ACCESS"); ok {
		if err := testDirectDataAccess.Parse(directs); err != nil {
			logger.Fatalf("NESSIE_TEST_DATA_ACCESS=\"%s\": %s", directs, err)
		}
	}

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
