package esti

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/go-openapi/swag"
	"github.com/hashicorp/go-multierror"
	"github.com/spf13/viper"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/testutil"
)

const (
	DefaultAdminAccessKeyID     = "AKIAIOSFDNN7EXAMPLEQ"
	DefaultAdminSecretAccessKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
)

type (
	Booleans   []bool
	arrayFlags []string
)

var (
	logger      logging.Logger
	client      apigen.ClientWithResponsesInterface
	endpointURL string
	svc         *s3.Client
	server      *webhookServer

	metaClientJarPath  string
	sparkImageTag      string
	repositoriesToKeep arrayFlags
	groupsToKeep       arrayFlags
	usersToKeep        arrayFlags
	policiesToKeep     arrayFlags
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

func envCleanup(client apigen.ClientWithResponsesInterface, repositoriesToKeep, groupsToKeep, usersToKeep, policiesToKeep arrayFlags) error {
	ctx := context.Background()
	errRepos := deleteAllRepositories(ctx, client, repositoriesToKeep)
	errGroups := deleteAllGroups(ctx, client, groupsToKeep)
	errPolicies := deleteAllPolicies(ctx, client, policiesToKeep)
	errUsers := deleteAllUsers(ctx, client, usersToKeep)
	return multierror.Append(errRepos, errGroups, errPolicies, errUsers).ErrorOrNil()
}

func deleteAllRepositories(ctx context.Context, client apigen.ClientWithResponsesInterface, repositoriesToKeep arrayFlags) error {
	// collect repositories to delete
	var (
		repositoriesToDelete []string
		nextOffset           string
	)

	for {
		resp, err := client.ListRepositoriesWithResponse(ctx, &apigen.ListRepositoriesParams{After: apiutil.Ptr(apigen.PaginationAfter(nextOffset))})
		if err != nil {
			return fmt.Errorf("list repositories: %w", err)
		}
		if resp.StatusCode() != http.StatusOK {
			return fmt.Errorf("list repositories: status: %s", resp.Status())
		}
		for _, repo := range resp.JSON200.Results {
			if !slices.Contains(repositoriesToKeep, repo.Id) {
				repositoriesToDelete = append(repositoriesToDelete, repo.Id)
			}
		}
		if !resp.JSON200.Pagination.HasMore {
			break
		}
		nextOffset = resp.JSON200.Pagination.NextOffset
	}

	var errs *multierror.Error
	for _, id := range repositoriesToDelete {
		resp, err := client.DeleteRepositoryWithResponse(ctx, id, &apigen.DeleteRepositoryParams{})
		if err != nil {
			errs = multierror.Append(errs, fmt.Errorf("delete repository: %s, err: %w", id, err))
		} else if resp.StatusCode() != http.StatusNoContent {
			errs = multierror.Append(errs, fmt.Errorf("delete repository: %s, status: %s", id, resp.Status()))
		}

	}
	return errs.ErrorOrNil()
}

func deleteAllGroups(ctx context.Context, client apigen.ClientWithResponsesInterface, groupsToKeep arrayFlags) error {
	// list groups to delete
	var (
		groupsToDelete []string
		nextOffset     string
	)
	for {
		resp, err := client.ListGroupsWithResponse(ctx, &apigen.ListGroupsParams{After: apiutil.Ptr(apigen.PaginationAfter(nextOffset))})
		if err != nil {
			return fmt.Errorf("list groups: %w", err)
		}
		if resp.StatusCode() != http.StatusOK {
			return fmt.Errorf("list groups: status: %s", resp.Status())
		}
		for _, group := range resp.JSON200.Results {
			if !slices.Contains(groupsToKeep, swag.StringValue(group.Name)) {
				groupsToDelete = append(groupsToDelete, group.Id)
			}
		}
		if !resp.JSON200.Pagination.HasMore {
			break
		}
		nextOffset = resp.JSON200.Pagination.NextOffset
	}

	// delete groups
	var errs *multierror.Error
	for _, id := range groupsToDelete {
		resp, err := client.DeleteGroupWithResponse(ctx, id)
		if err != nil {
			errs = multierror.Append(errs, fmt.Errorf("delete group: %s, err: %w", id, err))
		} else if resp.StatusCode() != http.StatusNoContent {
			errs = multierror.Append(errs, fmt.Errorf("delete group: %s, status: %s", id, resp.Status()))
		}
	}
	return errs.ErrorOrNil()
}

func deleteAllUsers(ctx context.Context, client apigen.ClientWithResponsesInterface, usersToKeep arrayFlags) error {
	// collect users to delete
	var (
		usersToDelete []string
		nextOffset    string
	)
	for {
		resp, err := client.ListUsersWithResponse(ctx, &apigen.ListUsersParams{After: apiutil.Ptr(apigen.PaginationAfter(nextOffset))})
		if err != nil {
			return fmt.Errorf("list users: %s", err)
		}
		if resp.JSON200 == nil {
			return fmt.Errorf("list users, status: %s", resp.Status())
		}
		for _, user := range resp.JSON200.Results {
			if !slices.Contains(usersToKeep, user.Id) {
				usersToKeep = append(usersToDelete, user.Id)
			}
		}
		if !resp.JSON200.Pagination.HasMore {
			break
		}
		nextOffset = resp.JSON200.Pagination.NextOffset
	}

	// delete users
	var errs *multierror.Error
	for _, id := range usersToDelete {
		resp, err := client.DeleteUserWithResponse(ctx, id)
		if err != nil {
			errs = multierror.Append(errs, fmt.Errorf("delete user %s: %w", id, err))
		} else if resp.StatusCode() != http.StatusNoContent {
			errs = multierror.Append(errs, fmt.Errorf("delete user %s, status: %s", id, resp.Status()))
		}
	}
	return errs.ErrorOrNil()
}

func deleteAllPolicies(ctx context.Context, client apigen.ClientWithResponsesInterface, policiesToKeep arrayFlags) error {
	// list policies to delete
	var (
		policiesToDelete []string
		nextOffset       string
	)
	for {
		resp, err := client.ListPoliciesWithResponse(ctx, &apigen.ListPoliciesParams{After: apiutil.Ptr(apigen.PaginationAfter(nextOffset))})
		if err != nil {
			return fmt.Errorf("list policies: %w", err)
		}
		if resp.JSON200 == nil {
			return fmt.Errorf("list policies, status: %s", resp.Status())
		}
		for _, policy := range resp.JSON200.Results {
			if !slices.Contains(policiesToKeep, policy.Id) {
				policiesToDelete = append(policiesToDelete, policy.Id)
			}
		}
		if !resp.JSON200.Pagination.HasMore {
			break
		}
		nextOffset = resp.JSON200.Pagination.NextOffset
	}

	// delete policies
	var errs *multierror.Error
	for _, id := range policiesToDelete {
		resp, err := client.DeletePolicyWithResponse(ctx, id)
		if err != nil {
			errs = multierror.Append(errs, fmt.Errorf("delete policy %s: %w", id, err))
		} else if resp.StatusCode() != http.StatusNoContent {
			errs = multierror.Append(errs, fmt.Errorf("delete policy %s, status: %s", id, resp.Status()))
		}
	}
	return errs.ErrorOrNil()
}

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
		Name:      "esti",
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
		err := envCleanup(client, repositoriesToKeep, groupsToKeep, usersToKeep, policiesToKeep)
		if err != nil {
			logger.WithError(err).Fatal("env cleanup")
		}
	}

	var err error
	server, err = startWebhookServer()
	if err != nil {
		logger.WithError(err).Fatal("start webhook server")
	}
	defer func() { _ = server.s.Close() }()

	logger.Info("Setup succeeded, running the tests")
	os.Exit(m.Run())
}
