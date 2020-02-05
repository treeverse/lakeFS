package api

import (
	"context"
	"net/url"

	"github.com/go-openapi/runtime"
	httptransport "github.com/go-openapi/runtime/client"
	"github.com/go-openapi/strfmt"
	"github.com/treeverse/lakefs/api/gen/client/operations"

	genclient "github.com/treeverse/lakefs/api/gen/client"
	"github.com/treeverse/lakefs/api/gen/models"
)

/*

GET    /repositories
GET    /repositories/myrepo
POST   /repositories/myrepo
DELETE /repositories/myrepo

GET    /repositories/myrepo/branches
GET    /repositories/myrepo/branches/feature-new
POST   /repositories/myrepo/branches/feature-new
DELETE /repositories/myrepo/branches/feature-new

GET    /repositories/myrepo/branches/feature-new/stat/collections/file.csv
GET    /repositories/myrepo/branches/feature-new/ls/prefix?from="<from_path>"
GET    /repositories/myrepo/branches/feature-new/objects/collections/file.csv
PUT    /repositories/myrepo/branches/feature-new/objects/collections/file.csv
DELETE /repositories/myrepo/branches/feature-new/objects/collections/file.csv

POST /repositories/myrepo/branches/feature-new/commits
GET  /repositories/myrepo/branches/feature-new/commits
GET  /repositories/myrepo/commits/commit_id

GET  /repositories/myrepo/branches/feature-new/diff/master
PUT  /repositories/myrepo/branches/feature-new/checkout/collections/file.csv
PUT  /repositories/myrepo/branches/feature-new/reset
PUT  /repositories/myrepo/branches/feature-new/merge/master

*/

type Client interface {
	ListRepositories(ctx context.Context) ([]*models.Repository, error)
	GetRepository(ctx context.Context, repoId string) (*models.Repository, error)
	CreateRepository(ctx context.Context, repoId string, repository *models.RepositoryCreation) error
	DeleteRepository(ctx context.Context, repoId string) error

	ListBranches(ctx context.Context, repoId string) ([]*models.Refspec, error)
	GetBranch(ctx context.Context, repoId, branchId string) (*models.Refspec, error)
	CreateBranch(ctx context.Context, repoId, branchId string, branch *models.Refspec) error
	DeleteBranch(ctx context.Context, repoId, branchId string) error
}

type client struct {
	remote *genclient.Lakefs
	auth   runtime.ClientAuthInfoWriter
}

func (c *client) ListRepositories(ctx context.Context) ([]*models.Repository, error) {
	resp, err := c.remote.Operations.ListRepositories(&operations.ListRepositoriesParams{
		Context: ctx,
	}, c.auth)
	if err != nil {
		return nil, err
	}
	return resp.GetPayload(), nil
}

func (c *client) GetRepository(ctx context.Context, repoId string) (*models.Repository, error) {
	resp, err := c.remote.Operations.GetRepository(&operations.GetRepositoryParams{
		RepositoryID: repoId,
		Context:      ctx,
	}, c.auth)
	if err != nil {
		return nil, err
	}
	return resp.GetPayload(), nil
}

func (c *client) ListBranches(ctx context.Context, repoId string) ([]*models.Refspec, error) {
	resp, err := c.remote.Operations.ListBranches(&operations.ListBranchesParams{
		RepositoryID: repoId,
		Context:      context.Background(),
	}, c.auth)
	if err != nil {
		return nil, err
	}
	return resp.GetPayload(), nil
}

func (c *client) CreateRepository(ctx context.Context, repoId string, repository *models.RepositoryCreation) error {
	_, err := c.remote.Operations.CreateRepository(&operations.CreateRepositoryParams{
		Repository:   repository,
		RepositoryID: repoId,
		Context:      ctx,
	}, c.auth)
	return err
}

func (c *client) DeleteRepository(ctx context.Context, repoId string) error {
	_, err := c.remote.Operations.DeleteRepository(&operations.DeleteRepositoryParams{
		RepositoryID: repoId,
		Context:      ctx,
	}, c.auth)
	return err
}

func (c *client) GetBranch(ctx context.Context, repoId, branchId string) (*models.Refspec, error) {
	resp, err := c.remote.Operations.GetBranch(&operations.GetBranchParams{
		BranchID:     branchId,
		RepositoryID: repoId,
		Context:      ctx,
	}, c.auth)
	if err != nil {
		return nil, err
	}
	return resp.GetPayload(), nil
}

func (c *client) CreateBranch(ctx context.Context, repoId, branchId string, branch *models.Refspec) error {
	_, err := c.remote.Operations.CreateBranch(&operations.CreateBranchParams{
		Branch:       branch,
		BranchID:     branchId,
		RepositoryID: repoId,
		Context:      ctx,
	}, c.auth)
	return err
}

func (c *client) DeleteBranch(ctx context.Context, repoId, branchId string) error {
	_, err := c.remote.Operations.DeleteBranch(&operations.DeleteBranchParams{
		BranchID:     branchId,
		RepositoryID: repoId,
		Context:      ctx,
	}, c.auth)
	return err
}

func NewClient(endpointURL, accessKeyId, secretAccessKey string) (Client, error) {
	parsedUrl, err := url.Parse(endpointURL)
	if err != nil {
		return nil, err
	}
	return &client{
		remote: genclient.New(httptransport.New(parsedUrl.Host, parsedUrl.Path, []string{parsedUrl.Scheme}), strfmt.Default),
		auth:   httptransport.BasicAuth(accessKeyId, secretAccessKey),
	}, nil
}
