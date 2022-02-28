package cmd

import (
	"context"
	"fmt"
	"strings"

	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/metastore"
)

// ExtractRepoAndBranchFromDBName extracts the repository and branch in which the metastore resides
func ExtractRepoAndBranchFromDBName(ctx context.Context, dbName string, client metastore.Client) (string, string, error) {
	metastoreDB, err := client.GetDatabase(ctx, dbName)

	if err != nil {
		return "", "", fmt.Errorf("failed to get database on extractBranchFromSchema from '%s': %w", dbName, err)
	}

	repo, branch, err := metastore.GetRepoAndBranchFromMSLocationURI(metastoreDB.LocationURI)
	if err != nil {
		return "", "", fmt.Errorf("failed to get source branch on extractBranchFromSchema from '%s': %w", metastoreDB.LocationURI, err)
	}

	return repo, branch, nil
}

// GenerateLakeFSBranchURIFromRepoAndBranchName generates a valid URI from the given repository and branch names
func GenerateLakeFSBranchURIFromRepoAndBranchName(repoName, branchName string) (string, error) {
	if len(strings.TrimSpace(repoName)) == 0 || len(strings.TrimSpace(branchName)) == 0 {
		return "", fmt.Errorf("failed to generate a valid URI string with repo \"%s\" and branch \"%s\"", repoName, branchName)
	}
	return fmt.Sprintf(`lakefs://%s/%s`, repoName, branchName), nil
}

// CreateBranch creates a new branch with the given source and destination branch URIs
func CreateBranch(ctx context.Context, sourceLakeFSBranchURI, destinationLakeFSBranchURI string) {
	branchURI := MustParseRefURI("destination branch URI", destinationLakeFSBranchURI)
	sourceURI := MustParseRefURI("source branch URI", sourceLakeFSBranchURI)

	client := getClient()
	resp, err := client.CreateBranchWithResponse(ctx, branchURI.Repository, api.CreateBranchJSONRequestBody{
		Name:   branchURI.Ref,
		Source: sourceURI.Ref,
	})
	DieOnResponseError(resp, err)
	Fmt("created branch '%s' %s\n", branchURI.Ref, string(resp.Body))
}
