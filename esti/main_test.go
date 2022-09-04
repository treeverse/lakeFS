package esti

import (
	"flag"
	"log"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/spf13/viper"
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

func (i *arrayFlags) String() string {
	return "my string representation"
}

func (i *arrayFlags) Set(value string) error {
	*i = append(*i, value)
	return nil
}

const (
	DefaultAdminAccessKeyId     = "AKIAIOSFDNN7EXAMPLEQ"
	DefaultAdminSecretAccessKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
)

var (
	logger logging.Logger
	client api.ClientWithResponsesInterface
	svc    *s3.S3
	server *webhookServer

	testDirectDataAccess = Booleans{false}

	repositoriesToKeep arrayFlags
	groupsToKeep       arrayFlags
	usersToKeep        arrayFlags
	policiesToKeep     arrayFlags
)

func TestMain(m *testing.M) {
	systemTests := flag.Bool("system-tests", false, "Run system tests")
	useLocalCredentials := flag.Bool("use-local-credentials", false, "Generate local API key during `lakefs setup'")
	adminAccessKeyID := flag.String("admin-access-key-id", DefaultAdminAccessKeyId, "lakeFS Admin access key ID")
	adminSecretAccessKey := flag.String("admin-secret-access-key", DefaultAdminSecretAccessKey, "lakeFS Admin secret access key")
	cleanupEnv := flag.Bool("cleanup-env-pre-run", false, "Clean repositories, groups, users and polices before sunning esti tests")
	flag.Var(&repositoriesToKeep, "repository-to-keep", "Repositories to keep in case of pre-run cleanup")
	flag.Var(&groupsToKeep, "group-to-keep", "Groups to keep in case of pre-run cleanup")
	flag.Var(&usersToKeep, "user-to-keep", "Users to keep in case of pre-run cleanup")
	flag.Var(&policiesToKeep, "policy-to-keep", "Policies to keep in case of pre-run cleanup")

	if directs, ok := os.LookupEnv("ESTI_TEST_DATA_ACCESS"); ok {
		if err := testDirectDataAccess.Parse(directs); err != nil {
			logger.Fatalf("ESTI_TEST_DATA_ACCESS=\"%s\": %s", directs, err)
		}
	}

	flag.Parse()
	if !*systemTests {
		os.Exit(0)
	}

	params := testutil.SetupTestingEnvParams{
		Name:      "esti",
		StorageNS: "esti-system-testing",
	}

	if *useLocalCredentials {
		params.AdminAccessKeyID = *adminAccessKeyID
		params.AdminSecretAccessKey = *adminSecretAccessKey
	}
	viper.SetDefault("post_migrate", false)

	logger, client, svc = testutil.SetupTestingEnv(&params)

	var err error
	if *cleanupEnv {
		logger.Infof("Deleting Repositories, groups, users and policies before Esti run. Repositories to keep: %s, groups to keep: %s, users to keep: %s, policies to keep: %s", repositoriesToKeep, groupsToKeep, usersToKeep, policiesToKeep)

		err = envCleanup(client, repositoriesToKeep, groupsToKeep, usersToKeep, policiesToKeep, logger)
		if err != nil {
			log.Fatal(err)
		}
	}

	server, err = startWebhookServer()
	if err != nil {
		log.Fatal(err)
	}

	defer func() { _ = server.s.Close() }()

	logger.Info("Setup succeeded, running the tests")
	os.Exit(m.Run())
}
