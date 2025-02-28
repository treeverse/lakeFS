package esti

import (
	"flag"
	"os"
	"testing"

	"github.com/spf13/viper"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/testutil"
)

func TestMain(m *testing.M) {
	systemTests := flag.Bool("system-tests", false, "Run system tests")
	useLocalCredentials := flag.Bool("use-local-credentials", false, "Generate local API key during `lakefs setup'")
	adminAccessKeyID := flag.String("admin-access-key-id", DefaultAdminAccessKeyID, "lakeFS Admin access key ID")
	adminSecretAccessKey := flag.String("admin-secret-access-key", DefaultAdminSecretAccessKey, "lakeFS Admin secret access key")
	cleanupEnv := flag.Bool("cleanup-env-pre-run", false, "Clean repositories, groups, users and polices before running esti tests")
	flag.Var(&repositoriesToKeep, "repository-to-keep", "Repositories to keep in case of pre-run cleanup")
	flag.Var(&groupsToKeep, "group-to-keep", "Groups to keep in case of pre-run cleanup")
	flag.Var(&usersToKeep, "user-to-keep", "Users to keep in case of pre-run cleanup")
	flag.Var(&policiesToKeep, "policy-to-keep", "Policies to keep in case of pre-run cleanup")
	flag.StringVar(&metaClientJarPath, "metaclient-jar", "", "Location of the lakeFS metadata client jar")
	flag.StringVar(&sparkImageTag, "spark-image-tag", "", "Tag of Bitnami Spark image")
	flag.Parse()

	if !*systemTests {
		os.Exit(0)
	}

	params := testutil.SetupTestingEnvParams{
		Name:      AdminUsername,
		StorageNS: "esti-system-testing",
	}

	if *useLocalCredentials {
		params.AdminAccessKeyID = *adminAccessKeyID
		params.AdminSecretAccessKey = *adminSecretAccessKey
	}

	logger, client, svc, endpointURL = testutil.SetupTestingEnv(&params)

	setupLakeFS := viper.GetBool("setup_lakefs")
	if !setupLakeFS && *cleanupEnv {
		logger.WithFields(logging.Fields{
			"repositories": repositoriesToKeep,
			"groups":       groupsToKeep,
			"users":        usersToKeep,
			"policies":     policiesToKeep,
		}).Info("Deleting repositories, groups, users and policies before Esti run")
		err := EnvCleanup(client, repositoriesToKeep, groupsToKeep, usersToKeep, policiesToKeep)
		if err != nil {
			logger.WithError(err).Fatal("env cleanup")
		}
	}

	var err error
	server, err = StartWebhookServer()
	if err != nil {
		logger.WithError(err).Fatal("start webhook server")
	}
	defer func() { _ = server.Server().Close() }()

	logger.Info("Setup succeeded, running the tests")
	os.Exit(m.Run())
}
