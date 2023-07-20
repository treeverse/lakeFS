package cmd

import (
	"context"
	"fmt"
	"net/http"

	"github.com/treeverse/lakefs/cmd/lakectl/cmd/utils"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/metastore"
)

const branchCreatedTemplate = `created branch "{{.Branch}}" {{.Resp}}
`

// ExtractRepoAndBranchFromDBName extracts the repository and branch in which the metastore resides
func ExtractRepoAndBranchFromDBName(ctx context.Context, dbName string, client metastore.Client) (string, string, error) {
	metastoreDB, err := client.GetDatabase(ctx, dbName)
	if err != nil {
		return "", "", err
	}

	repo, branch, err := metastore.ExtractRepoAndBranch(metastoreDB.LocationURI)
	if err != nil {
		return "", "", fmt.Errorf("get source branch from '%s': %w", metastoreDB.LocationURI, err)
	}

	return repo, branch, nil
}

// CreateBranch creates a new branch with the given repository, and source and destination branch names
func CreateBranch(ctx context.Context, repository, sourceBranch, destinationBranch string) {
	client := getClient()
	resp, err := client.CreateBranchWithResponse(ctx, repository, api.CreateBranchJSONRequestBody{
		Name:   destinationBranch,
		Source: sourceBranch,
	})
	utils.DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusCreated)
	utils.Write(branchCreatedTemplate, struct {
		Branch string
		Resp   string
	}{
		Branch: destinationBranch,
		Resp:   string(resp.Body),
	})
}
