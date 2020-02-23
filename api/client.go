package api

import (
	"context"
	"net/url"

	"github.com/go-openapi/swag"

	"github.com/go-openapi/runtime"
	httptransport "github.com/go-openapi/runtime/client"
	"github.com/go-openapi/strfmt"
	"github.com/treeverse/lakefs/api/gen/client/branches"
	"github.com/treeverse/lakefs/api/gen/client/commits"
	"github.com/treeverse/lakefs/api/gen/client/repositories"

	genclient "github.com/treeverse/lakefs/api/gen/client"
	"github.com/treeverse/lakefs/api/gen/models"
)

/*

GET    /repositories // list repos
GET    /repositories/myrepo // get repo
POST   /repositories/myrepo // create repo
DELETE /repositories/myrepo // delete repo

GET    /repositories/myrepo/branches // list branches
GET    /repositories/myrepo/branches/feature-new // get branch
POST   /repositories/myrepo/branches/feature-new // create branch
PUT 	/repositories/myrepo/branches/feature-new?[commit=<commit_id>] [path=<path>] // revert branch to previous commit or revert path to last commit
DELETE /repositories/myrepo/branches/feature-new // delete branch

GET    /repositories/myrepo/branches/feature-new/stat/collections/file.csv // get file metadata
GET    /repositories/myrepo/branches/feature-new/ls/prefix?from="<from_path>" // list files
GET    /repositories/myrepo/branches/feature-new/objects/collections/file.csv // get file content
PUT    /repositories/myrepo/branches/feature-new/objects/collections/file.csv // upload file content
DELETE /repositories/myrepo/branches/feature-new/objects/collections/file.csv // delete file

POST /repositories/myrepo/branches/feature-new/commits // create a commit as head of branch
GET  /repositories/myrepo/branches/feature-new/commits // list commits for branch
GET  /repositories/myrepo/commits/commit_id // get commit info

GET  /repositories/myrepo/branches/feature-new/diff/master // get diff between branches
PUT  /repositories/myrepo/branches/feature-new/checkout/collections/file.csv // checkout a given file (i.e. restore it to last committed version)
PUT  /repositories/myrepo/branches/feature-new/reset // reset (i.e. restore tree to last committed version)
PUT  /repositories/myrepo/branches/feature-new/merge/master // merge branch into destination

*/

type Client interface {
	ListRepositories(ctx context.Context, from string, amount int) ([]*models.Repository, *models.Pagination, error)
	GetRepository(ctx context.Context, repoId string) (*models.Repository, error)
	CreateRepository(ctx context.Context, repository *models.RepositoryCreation) error
	DeleteRepository(ctx context.Context, repoId string) error

	ListBranches(ctx context.Context, repoId string, from string, amount int) ([]*models.Refspec, *models.Pagination, error)
	GetBranch(ctx context.Context, repoId, branchId string) (*models.Refspec, error)
	CreateBranch(ctx context.Context, repoId string, branch *models.Refspec) error
	DeleteBranch(ctx context.Context, repoId, branchId string) error
	RevertBranch(ctx context.Context, repoId, branchId string, revertProps *models.RevertCreation) error

	Commit(ctx context.Context, repoId, branchId, message string, metadata map[string]string) (*models.Commit, error)
	GetCommit(ctx context.Context, repoId, commitId string) (*models.Commit, error)
	GetCommitLog(ctx context.Context, repoId, branchId string) ([]*models.Commit, error)

	DiffBranches(ctx context.Context, repoId, branch, otherBranch string) ([]*models.Diff, error)
	DiffBranch(ctx context.Context, repoId, branch string) ([]*models.Diff, error)
}

type client struct {
	remote *genclient.Lakefs
	auth   runtime.ClientAuthInfoWriter
}

func (c *client) ListRepositories(ctx context.Context, from string, amount int) ([]*models.Repository, *models.Pagination, error) {
	resp, err := c.remote.Repositories.ListRepositories(&repositories.ListRepositoriesParams{
		After:   swag.String(from),
		Amount:  swag.Int64(int64(amount)),
		Context: ctx,
	}, c.auth)
	if err != nil {
		return nil, nil, err
	}
	return resp.GetPayload().Results, resp.GetPayload().Pagination, nil
}

func (c *client) GetRepository(ctx context.Context, repoId string) (*models.Repository, error) {
	resp, err := c.remote.Repositories.GetRepository(&repositories.GetRepositoryParams{
		RepositoryID: repoId,
		Context:      ctx,
	}, c.auth)
	if err != nil {
		return nil, err
	}
	return resp.GetPayload(), nil
}

func (c *client) ListBranches(ctx context.Context, repoId string, from string, amount int) ([]*models.Refspec, *models.Pagination, error) {
	resp, err := c.remote.Branches.ListBranches(&branches.ListBranchesParams{
		After:        swag.String(from),
		Amount:       swag.Int64(int64(amount)),
		RepositoryID: repoId,
		Context:      ctx,
	}, c.auth)
	if err != nil {
		return nil, nil, err
	}
	return resp.GetPayload().Results, resp.GetPayload().Pagination, nil
}

func (c *client) CreateRepository(ctx context.Context, repository *models.RepositoryCreation) error {
	_, err := c.remote.Repositories.CreateRepository(&repositories.CreateRepositoryParams{
		Repository: repository,
		Context:    ctx,
	}, c.auth)
	return err
}

func (c *client) DeleteRepository(ctx context.Context, repoId string) error {
	_, err := c.remote.Repositories.DeleteRepository(&repositories.DeleteRepositoryParams{
		RepositoryID: repoId,
		Context:      ctx,
	}, c.auth)
	return err
}

func (c *client) GetBranch(ctx context.Context, repoId, branchId string) (*models.Refspec, error) {
	resp, err := c.remote.Branches.GetBranch(&branches.GetBranchParams{
		BranchID:     branchId,
		RepositoryID: repoId,
		Context:      ctx,
	}, c.auth)
	if err != nil {
		return nil, err
	}
	return resp.GetPayload(), nil
}

func (c *client) CreateBranch(ctx context.Context, repoId string, branch *models.Refspec) error {
	_, err := c.remote.Branches.CreateBranch(&branches.CreateBranchParams{
		Branch:       branch,
		RepositoryID: repoId,
		Context:      ctx,
	}, c.auth)
	return err
}

func (c *client) DeleteBranch(ctx context.Context, repoId, branchId string) error {
	_, err := c.remote.Branches.DeleteBranch(&branches.DeleteBranchParams{
		BranchID:     branchId,
		RepositoryID: repoId,
		Context:      ctx,
	}, c.auth)
	return err
}
func (c *client) RevertBranch(ctx context.Context, repoId, branchId string, revertProps *models.RevertCreation) error {
	_, err := c.remote.Branches.RevertBranch(&branches.RevertBranchParams{
		BranchID:     branchId,
		Revert:       revertProps,
		RepositoryID: repoId,
		Context:      ctx,
	}, c.auth)
	return err
}

func (c *client) Commit(ctx context.Context, repoId, branchId, message string, metadata map[string]string) (*models.Commit, error) {
	commit, err := c.remote.Commits.Commit(&commits.CommitParams{
		BranchID: branchId,
		Commit: &models.CommitCreation{
			Message:  &message,
			Metadata: metadata,
		},
		RepositoryID: repoId,
		Context:      ctx,
	}, c.auth)
	if err != nil {
		return nil, err
	}
	return commit.GetPayload(), nil
}

func (c *client) GetCommit(ctx context.Context, repoId, commitId string) (*models.Commit, error) {
	commit, err := c.remote.Commits.GetCommit(&commits.GetCommitParams{
		CommitID:     commitId,
		RepositoryID: repoId,
		Context:      ctx,
	}, c.auth)
	if err != nil {
		return nil, err
	}
	return commit.GetPayload(), nil
}

func (c *client) GetCommitLog(ctx context.Context, repoId, branchId string) ([]*models.Commit, error) {
	log, err := c.remote.Commits.GetBranchCommitLog(&commits.GetBranchCommitLogParams{
		BranchID:     branchId,
		RepositoryID: repoId,
		Context:      ctx,
	}, c.auth)
	if err != nil {
		return nil, err
	}
	return log.GetPayload().Results, nil
}

func (c *client) DiffBranches(ctx context.Context, repoId, branch, otherBranch string) ([]*models.Diff, error) {
	diff, err := c.remote.Branches.DiffBranches(&branches.DiffBranchesParams{
		BranchID:      branch,
		OtherBranchID: otherBranch,
		RepositoryID:  repoId,
		Context:       ctx,
	}, c.auth)
	if err != nil {
		return nil, err
	}
	return diff.GetPayload().Results, nil
}

func (c *client) DiffBranch(ctx context.Context, repoId, branch string) ([]*models.Diff, error) {
	diff, err := c.remote.Branches.DiffBranch(&branches.DiffBranchParams{
		BranchID:     branch,
		RepositoryID: repoId,
		Context:      ctx,
	}, c.auth)
	if err != nil {
		return nil, err
	}
	return diff.GetPayload().Results, nil
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
