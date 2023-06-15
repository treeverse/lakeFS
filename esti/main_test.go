package esti

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/spf13/viper"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/testutil"
)

type (
	Booleans   []bool
	arrayFlags []string
)

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
	return strings.Join(*i, " ")
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
	logger      logging.Logger
	client      api.ClientWithResponsesInterface
	endpointURL string
	svc         *s3.S3
	server      *webhookServer

	testDirectDataAccess = Booleans{false}

	metaclientJarPath  string
	sparkImageTag      string
	repositoriesToKeep arrayFlags
	groupsToKeep       arrayFlags
	usersToKeep        arrayFlags
	policiesToKeep     arrayFlags

	testsToSkipRegex *regexp.Regexp
)

var (
	azureStorageAccount   string
	azureStorageAccessKey string
)

func arrayFlagsToMap(arr arrayFlags) map[interface{}]bool {
	retMap := map[interface{}]bool{}
	for _, element := range arr {
		retMap[element] = true
	}
	return retMap
}

func envCleanup(client api.ClientWithResponsesInterface, repositoriesToKeep, groupsToKeep, usersToKeep, policiesToKeep arrayFlags, logger logging.Logger) error {
	ctx := context.Background()
	err := deleteAllRepositories(ctx, client, repositoriesToKeep, logger)
	if err != nil {
		return err
	}

	err = deleteAllGroups(ctx, client, groupsToKeep, logger)
	if err != nil {
		return err
	}

	err = deleteAllPolicies(ctx, client, policiesToKeep, logger)
	if err != nil {
		return err
	}

	err = deleteAllUsers(ctx, client, usersToKeep, logger)
	return err
}

func deleteAllRepositories(ctx context.Context, client api.ClientWithResponsesInterface, repositoriesToKeep arrayFlags, logger logging.Logger) error {
	repositoriesToNotDelete := arrayFlagsToMap(repositoriesToKeep)
	listRepositoriesResponse, err := client.ListRepositoriesWithResponse(ctx, &api.ListRepositoriesParams{})
	if err != nil {
		return err
	}
	if listRepositoriesResponse.StatusCode() != http.StatusOK {
		return fmt.Errorf("unexpected status code for list repositories request: %d", listRepositoriesResponse.StatusCode())
	}
	hasMore := true
	for hasMore {
		for _, repo := range listRepositoriesResponse.JSON200.Results {
			if !repositoriesToNotDelete[repo.Id] {
				deleteRepositoryResponse, err := client.DeleteRepositoryWithResponse(ctx, repo.Id)
				if err != nil {
					logger.Warnf("failed to delete repository: %s. err: %s", repo.Id, err.Error())
					continue
				}
				if deleteRepositoryResponse.StatusCode() != http.StatusNoContent {
					logger.Warnf("unexpected status code when trying to delete repository: %s, status code: %s", repo.Id, deleteRepositoryResponse.Status())
				}
			}
		}
		hasMore = listRepositoriesResponse.JSON200.Pagination.HasMore
		if hasMore {
			nextOffset := api.PaginationAfter(listRepositoriesResponse.JSON200.Pagination.NextOffset)
			listRepositoriesResponse, err = client.ListRepositoriesWithResponse(ctx, &api.ListRepositoriesParams{After: &nextOffset})
			if err != nil {
				return err
			}
			if listRepositoriesResponse.StatusCode() != http.StatusOK {
				return fmt.Errorf("unexpected status code for list repositories request: %d", listRepositoriesResponse.StatusCode())
			}
		}
	}
	return err
}

func deleteAllGroups(ctx context.Context, client api.ClientWithResponsesInterface, groupsToKeep arrayFlags, logger logging.Logger) error {
	groupsToNotDelete := arrayFlagsToMap(groupsToKeep)

	listGroupsResponse, err := client.ListGroupsWithResponse(ctx, &api.ListGroupsParams{})
	if err != nil {
		return err
	}
	if listGroupsResponse.StatusCode() != http.StatusOK {
		return fmt.Errorf("unexpected status code for list groups request: %d", listGroupsResponse.StatusCode())
	}
	hasMore := true
	for hasMore {
		for _, group := range listGroupsResponse.JSON200.Results {
			if !groupsToNotDelete[group.Id] {
				deleteGroupResponse, err := client.DeleteGroupWithResponse(ctx, group.Id)
				if err != nil {
					logger.Warnf("failed to delete group: %s. err: %s", group.Id, err.Error())
				}
				if deleteGroupResponse.StatusCode() != http.StatusNoContent {
					logger.Warnf("unexpected status code when trying to delete group: %s, status code: %d", group.Id, deleteGroupResponse.StatusCode())
				}
			}
		}
		hasMore = listGroupsResponse.JSON200.Pagination.HasMore
		if hasMore {
			nextOffset := api.PaginationAfter(listGroupsResponse.JSON200.Pagination.NextOffset)
			listGroupsResponse, err = client.ListGroupsWithResponse(ctx, &api.ListGroupsParams{After: &nextOffset})
			if err != nil {
				return err
			}
			if listGroupsResponse.StatusCode() != http.StatusOK {
				return fmt.Errorf("unexpected status code for list groups request: %d", listGroupsResponse.StatusCode())
			}
		}
	}
	return err
}

func deleteAllUsers(ctx context.Context, client api.ClientWithResponsesInterface, usersToKeep arrayFlags, logger logging.Logger) error {
	usersToNotDelete := arrayFlagsToMap(usersToKeep)

	listUsersResponse, err := client.ListUsersWithResponse(ctx, &api.ListUsersParams{})
	if err != nil {
		return err
	}
	if listUsersResponse.StatusCode() != http.StatusOK {
		return fmt.Errorf("unexpected status code for list users request: %d", listUsersResponse.StatusCode())
	}
	hasMore := true
	for hasMore {
		for _, user := range listUsersResponse.JSON200.Results {
			if !usersToNotDelete[user.Id] {
				deleteUserResponse, err := client.DeleteUserWithResponse(ctx, user.Id)
				if err != nil {
					logger.Warnf("failed to delete user: %s. err: %s", user.Id, err.Error())
				}
				if deleteUserResponse.StatusCode() != http.StatusNoContent {
					logger.Warnf("unexpected status code when trying to delete user: %s, status code: %d", user.Id, deleteUserResponse.StatusCode())
				}
			}
		}
		hasMore = listUsersResponse.JSON200.Pagination.HasMore
		if hasMore {
			nextOffset := api.PaginationAfter(listUsersResponse.JSON200.Pagination.NextOffset)
			listUsersResponse, err = client.ListUsersWithResponse(ctx, &api.ListUsersParams{After: &nextOffset})
			if err != nil {
				return err
			}
			if listUsersResponse.StatusCode() != http.StatusOK {
				return fmt.Errorf("unexpected status code for list users request: %d", listUsersResponse.StatusCode())
			}
		}
	}
	return err
}

func deleteAllPolicies(ctx context.Context, client api.ClientWithResponsesInterface, policiesToKeep arrayFlags, logger logging.Logger) error {
	policiesToNotDelete := arrayFlagsToMap(policiesToKeep)

	listPoliciesResponse, err := client.ListPoliciesWithResponse(ctx, &api.ListPoliciesParams{})
	if err != nil {
		return err
	}
	if listPoliciesResponse.StatusCode() != http.StatusOK {
		return fmt.Errorf("unexpected status code for list policies request: %d", listPoliciesResponse.StatusCode())
	}
	hasMore := true
	for hasMore {
		for _, policy := range listPoliciesResponse.JSON200.Results {
			if !policiesToNotDelete[policy.Id] {
				deletePolicyResponse, err := client.DeletePolicyWithResponse(ctx, policy.Id)
				if err != nil {
					logger.Warnf("failed to delete policy: %s. err: %s", policy.Id, err.Error())
				}
				if deletePolicyResponse.StatusCode() != http.StatusNoContent {
					logger.Warnf("unexpected status code when trying to delete policy: %s, status code: %d", policy.Id, deletePolicyResponse.StatusCode())
				}
			}
		}
		hasMore = listPoliciesResponse.JSON200.Pagination.HasMore
		if hasMore {
			nextOffset := api.PaginationAfter(listPoliciesResponse.JSON200.Pagination.NextOffset)
			listPoliciesResponse, err = client.ListPoliciesWithResponse(ctx, &api.ListPoliciesParams{After: &nextOffset})
			if err != nil {
				return err
			}
			if listPoliciesResponse.StatusCode() != http.StatusOK {
				return fmt.Errorf("unexpected status code for list policies request: %d", listPoliciesResponse.StatusCode())
			}
		}
	}
	return err
}

func TestMain(m *testing.M) {
	systemTests := flag.Bool("system-tests", false, "Run system tests")
	useLocalCredentials := flag.Bool("use-local-credentials", false, "Generate local API key during `lakefs setup'")
	adminAccessKeyID := flag.String("admin-access-key-id", DefaultAdminAccessKeyId, "lakeFS Admin access key ID")
	adminSecretAccessKey := flag.String("admin-secret-access-key", DefaultAdminSecretAccessKey, "lakeFS Admin secret access key")
	cleanupEnv := flag.Bool("cleanup-env-pre-run", false, "Clean repositories, groups, users and polices before running esti tests")
	testsToSkip := flag.String("skip", "", "Tests to skip in a regex format")
	flag.Var(&repositoriesToKeep, "repository-to-keep", "Repositories to keep in case of pre-run cleanup")
	flag.Var(&groupsToKeep, "group-to-keep", "Groups to keep in case of pre-run cleanup")
	flag.Var(&usersToKeep, "user-to-keep", "Users to keep in case of pre-run cleanup")
	flag.Var(&policiesToKeep, "policy-to-keep", "Policies to keep in case of pre-run cleanup")
	flag.StringVar(&metaclientJarPath, "metaclient-jar", "", "Location of the lakeFS metadata client jar")
	flag.StringVar(&sparkImageTag, "spark-image-tag", "", "Tag of Bitnami Spark image")
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

	logger, client, svc, endpointURL = testutil.SetupTestingEnv(&params)
	azureStorageAccount = viper.GetString("azure_storage_account")
	azureStorageAccessKey = viper.GetString("azure_storage_access_key")

	var err error
	setupLakeFS := viper.GetBool("setup_lakefs")
	if !setupLakeFS && *cleanupEnv {
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

	if *testsToSkip != "" {
		testsToSkipRegex, err = regexp.Compile(*testsToSkip)
		if err != nil {
			log.Fatalf("Skip pattern '%s' failed to compile: %s", *testsToSkip, err)
		}
	}

	logger.Info("Setup succeeded, running the tests")
	os.Exit(m.Run())
}

func SkipTestIfAskedTo(t testing.TB) {
	if testsToSkipRegex != nil && testsToSkipRegex.MatchString(t.Name()) {
		t.SkipNow()
	}
}
