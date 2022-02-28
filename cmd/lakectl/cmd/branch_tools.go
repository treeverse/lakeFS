package cmd

import (
	"context"
	"fmt"

	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/metastore"
)

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

func GenerateLakeFSBranchURIFromRepoAndBranchName(repoName, branchName string) string {
	return fmt.Sprintf(`lakefs://%s/%s`, repoName, branchName)
}

func CreateBranch(ctx context.Context, sourceLakefsBranchUri, destinationLakefsBranchUri string) {
	branchURI := MustParseRefURI("destination branch uri", destinationLakefsBranchUri)
	sourceURI := MustParseRefURI("source branch uri", sourceLakefsBranchUri)

	client := getClient()
	resp, err := client.CreateBranchWithResponse(ctx, branchURI.Repository, api.CreateBranchJSONRequestBody{
		Name:   branchURI.Ref,
		Source: sourceURI.Ref,
	})
	DieOnResponseError(resp, err)
	Fmt("created branch '%s' %s\n", branchURI.Ref, string(resp.Body))
}
