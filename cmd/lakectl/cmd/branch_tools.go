package cmd

import (
	"context"
	"fmt"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/metastore"
	"github.com/treeverse/lakefs/pkg/uri"
)

func ExtractRepoAndBranchFromDBName(ctx context.Context, clientType, dbName string) (string, string, error) {
	client, deferFunc := getMetastoreClient(clientType, "")
	defer deferFunc()

	metastoreDB, err := client.GetDatabase(ctx, dbName)

	if err != nil {
		return "", "", fmt.Errorf("failed to get database on extractBranchFromSchema from '%s': %w", dbName, err)
	}

	repo, branch, err := metastore.GetRepoAndBranchFromMSLocationUri(metastoreDB.LocationURI)
	if err != nil {
		return "", "", fmt.Errorf("failed to get source branch on extractBranchFromSchema from '%s': %w", metastoreDB.LocationURI, err)
	}

	return repo, branch, nil
}

func GenerateLakefsBranchUriFromRepoAndBranchName(repoName, branchName string) string {
	return fmt.Sprintf(`%s://%s/%s`, uri.LakeFSSchema, repoName, branchName)
}

func CreateBranch(ctx context.Context, sourceLakefsBranchUri, destinationLakefsBranchUri string) {
	branchUri := MustParseRefURI("destination branch uri", destinationLakefsBranchUri)
	sourceUri := MustParseRefURI("source branch uri", sourceLakefsBranchUri)

	client := getClient()
	resp, err := client.CreateBranchWithResponse(ctx, branchUri.Repository, api.CreateBranchJSONRequestBody{
		Name:   branchUri.Ref,
		Source: sourceUri.Ref,
	})
	DieOnResponseError(resp, err)
	Fmt("created branch '%s' %s\n", branchUri.Ref, string(resp.Body))
}
