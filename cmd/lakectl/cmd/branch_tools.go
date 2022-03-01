package cmd

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/metastore"
	"github.com/treeverse/lakefs/pkg/uri"
)

var branchCreatedTemplate = `created branch "{{.Branch.Ref}}" {{.Resp}}
`

// MissingDBError means that the requested DB is missing/not found
type MissingDBError struct {
	dbName string
	err    error
}

func (mdbe MissingDBError) Error() string {
	return fmt.Sprintf("get database on extractBranchFromSchema from '%s'", mdbe.dbName)
}

func (mdbe MissingDBError) Unwrap() error {
	return mdbe.err
}

// ExtractSourceBranchError means that the source branch extraction from the provided location URI failed
type ExtractSourceBranchError struct {
	locationURI string
	err         error
}

func (esbe ExtractSourceBranchError) Error() string {
	return fmt.Sprintf("get source branch on extractBranchFromSchema from '%s'", esbe.locationURI)
}

func (esbe ExtractSourceBranchError) Unwrap() error {
	return esbe.err
}

type uriGenerationError struct {
	repo   string
	branch string
}

func (uge uriGenerationError) Error() string {
	return fmt.Sprintf("failed to generate a valid URI string with repo \"%s\" and branch \"%s\"", uge.repo, uge.branch)
}

// ExtractRepoAndBranchFromDBName extracts the repository and branch in which the metastore resides
func ExtractRepoAndBranchFromDBName(ctx context.Context, dbName string, client metastore.Client) (string, string, error) {
	metastoreDB, err := client.GetDatabase(ctx, dbName)

	if err != nil {
		return "", "", MissingDBError{dbName: dbName, err: err}
	}

	repo, branch, err := metastore.ExtractRepoAndBranch(metastoreDB.LocationURI)
	if err != nil {
		return "", "", ExtractSourceBranchError{locationURI: metastoreDB.LocationURI, err: err}
	}

	return repo, branch, nil
}

// GenerateLakeFSURI generates a valid URI from the given repository and branch names
func GenerateLakeFSURI(repoName, branchName string) (string, error) {
	if len(strings.TrimSpace(repoName)) == 0 || len(strings.TrimSpace(branchName)) == 0 {
		return "", uriGenerationError{repo: repoName, branch: branchName}
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
	DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusCreated)
	Write(branchCreatedTemplate, struct {
		Branch *uri.URI
		Resp   string
	}{
		Branch: branchURI, Resp: string(resp.Body),
	})
}
