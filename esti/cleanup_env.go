package esti

import (
	"context"
	"fmt"
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

func envCleanup(client api.ClientWithResponsesInterface, repositoriesToKeep, groupsToKeep, usersToKeep, policiesToKeep arrayFlags) error {
	ctx := context.Background()
	err := deleteAllRepositories(ctx, client, repositoriesToKeep)
	if err != nil {
		return err
	}

	err = deleteAllGroups(ctx, client, groupsToKeep)
	if err != nil {
		return err
	}

	err = deleteAllPolicies(ctx, client, policiesToKeep)
	if err != nil {
		return err
	}

	err = deleteAllUsers(ctx, client, usersToKeep)
	return err
}

func deleteAllRepositories(ctx context.Context, client api.ClientWithResponsesInterface, repositoriesToKeep arrayFlags) error {
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
				client.DeleteRepositoryWithResponse(ctx, repo.Id)
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

func deleteAllGroups(ctx context.Context, client api.ClientWithResponsesInterface, groupsToKeep arrayFlags) error {
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
				client.DeleteGroupWithResponse(ctx, group.Id)
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

func deleteAllUsers(ctx context.Context, client api.ClientWithResponsesInterface, usersToKeep arrayFlags) error {
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
				client.DeleteUserWithResponse(ctx, user.Id)
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

func deleteAllPolicies(ctx context.Context, client api.ClientWithResponsesInterface, policiesToKeep arrayFlags) error {
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
				client.DeletePolicyWithResponse(ctx, policy.Id)
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
