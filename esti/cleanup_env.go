package esti

import (
	"context"
	"fmt"
	"github.com/treeverse/lakefs/pkg/logging"
	"net/http"

	"github.com/treeverse/lakefs/pkg/api"
)

type arrayFlags []string

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
				if err != nil || deleteRepositoryResponse.StatusCode() != http.StatusOK {
					logger.Warnf("failed to delete repository: %s", repo.Id)
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
				if err != nil || deleteGroupResponse.StatusCode() != http.StatusOK {
					logger.Warnf("failed to delete group: %s", group.Id)
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
				if err != nil || deleteUserResponse.StatusCode() != http.StatusOK {
					logger.Warnf("failed to delete user: %s", user.Id)
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
				if err != nil || deletePolicyResponse.StatusCode() != http.StatusOK {
					logger.Warnf("failed to delete policy: %s", policy.Id)
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
