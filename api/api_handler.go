package api

import (
	"bytes"
	"net/http"

	"github.com/treeverse/lakefs/block"

	"github.com/treeverse/lakefs/api/gen/restapi/operations/branches"

	"github.com/treeverse/lakefs/api/gen/restapi/operations/commits"

	"github.com/go-openapi/runtime/middleware"
	"github.com/go-openapi/swag"
	"github.com/treeverse/lakefs/api/gen/models"
	"github.com/treeverse/lakefs/api/gen/restapi/operations"
	"github.com/treeverse/lakefs/api/gen/restapi/operations/repositories"
	"github.com/treeverse/lakefs/auth"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/ident"
	"github.com/treeverse/lakefs/index"
	"github.com/treeverse/lakefs/permissions"
	"golang.org/x/xerrors"
)

const (
	// Maximum amount of results returned for paginated queries to the API
	MaxResultsPerPage int64 = 1000
)

type Handler struct {
	meta         index.Index
	auth         auth.Service
	blockAdapter block.Adapter
}

func NewHandler(meta index.Index, auth auth.Service, blockAdapter block.Adapter) *Handler {
	return &Handler{
		meta:         meta,
		auth:         auth,
		blockAdapter: blockAdapter,
	}
}

// Configure attaches our API operations to a generated swagger API stub
// Adding new handlers requires also adding them here so that the generated server will use them
func (a *Handler) Configure(api *operations.LakefsAPI) {
	// Register operations here
	api.RepositoriesListRepositoriesHandler = a.ListRepositoriesHandler()
	api.RepositoriesGetRepositoryHandler = a.GetRepoHandler()
	api.RepositoriesCreateRepositoryHandler = a.CreateRepositoryHandler()
	api.RepositoriesDeleteRepositoryHandler = a.DeleteRepositoryHandler()

	api.BranchesListBranchesHandler = a.ListBranchesHandler()
	api.BranchesGetBranchHandler = a.GetBranchHandler()
	api.BranchesCreateBranchHandler = a.CreateBranchHandler()
	api.BranchesDeleteBranchHandler = a.DeleteBranchHandler()

	api.CommitsCommitHandler = a.CommitHandler()
	api.CommitsGetCommitHandler = a.GetCommitHandler()
}

func (a *Handler) authorize(user *models.User, perm permissions.Permission, arn string) error {
	return authorize(a.auth, user, perm, arn)
}

func (a *Handler) ListRepositoriesHandler() repositories.ListRepositoriesHandler {
	return repositories.ListRepositoriesHandlerFunc(func(params repositories.ListRepositoriesParams, user *models.User) middleware.Responder {
		err := a.authorize(user, permissions.ManageRepos, repoArn("*"))
		if err != nil {
			return repositories.NewListRepositoriesUnauthorized().WithPayload(responseErrorFrom(err))
		}

		// amount
		after := ""
		amount := MaxResultsPerPage
		if params.Amount != nil {
			amount = swag.Int64Value(params.Amount)
		}

		// paginate after
		if params.After != nil {
			after = swag.StringValue(params.After)
		}

		repos, hasMore, err := a.meta.ListRepos(int(amount), after)
		if err != nil {
			return repositories.NewListRepositoriesDefault(http.StatusInternalServerError).
				WithPayload(responseError("could not list repositories"))
		}

		repoList := make([]*models.Repository, len(repos))
		var lastId string
		for i, repo := range repos {
			repoList[i] = &models.Repository{
				BucketName:    repo.GetBucketName(),
				CreationDate:  repo.GetCreationDate(),
				DefaultBranch: repo.GetDefaultBranch(),
				ID:            repo.GetRepoId(),
			}
			lastId = repo.GetRepoId()
		}
		returnValue := repositories.NewListRepositoriesOK().WithPayload(&repositories.ListRepositoriesOKBody{
			Pagination: &models.Pagination{
				HasMore:    swag.Bool(hasMore),
				Results:    swag.Int64(int64(len(repoList))),
				MaxPerPage: swag.Int64(MaxResultsPerPage),
			},
			Results: repoList,
		})
		if hasMore {
			returnValue.Payload.Pagination.NextOffset = lastId
		}

		return returnValue
	})
}

func (a *Handler) GetRepoHandler() repositories.GetRepositoryHandler {
	return repositories.GetRepositoryHandlerFunc(func(params repositories.GetRepositoryParams, user *models.User) middleware.Responder {
		err := a.authorize(user, permissions.ManageRepos, repoArn("*"))
		if err != nil {
			return repositories.NewGetRepositoryUnauthorized().WithPayload(responseErrorFrom(err))
		}

		repo, err := a.meta.GetRepo(params.RepositoryID)
		if err != nil && xerrors.Is(err, db.ErrNotFound) {
			return repositories.NewGetRepositoryNotFound().
				WithPayload(responseError("repository not found"))
		} else if err != nil {
			return repositories.NewGetRepositoryDefault(http.StatusInternalServerError).
				WithPayload(responseError("error fetching repository"))
		}

		return repositories.NewGetRepositoryOK().
			WithPayload(&models.Repository{
				BucketName:    repo.GetBucketName(),
				CreationDate:  repo.GetCreationDate(),
				DefaultBranch: repo.GetDefaultBranch(),
				ID:            repo.GetRepoId(),
			})
	})
}

func (a *Handler) GetCommitHandler() commits.GetCommitHandler {
	return commits.GetCommitHandlerFunc(func(params commits.GetCommitParams, user *models.User) middleware.Responder {
		err := a.authorize(user, permissions.ManageRepos, repoArn(params.RepositoryID))
		if err != nil {
			return commits.NewGetCommitUnauthorized().WithPayload(responseErrorFrom(err))
		}
		commit, err := a.meta.GetCommit(params.RepositoryID, params.CommitID)

		if xerrors.Is(err, db.ErrNotFound) {
			return commits.NewGetCommitNotFound().WithPayload(responseError("commit not found"))
		}
		if err != nil {
			return commits.NewGetCommitDefault(http.StatusInternalServerError).WithPayload(responseErrorFrom(err))
		}
		return commits.NewGetCommitOK().WithPayload(&models.Commit{
			Committer:    commit.GetCommitter(),
			CreationDate: commit.GetTimestamp(),
			ID:           ident.Hash(commit),
			Message:      commit.GetMessage(),
			Metadata:     commit.GetMetadata(),
			Parents:      commit.GetParents(),
		})
	})
}

func (a *Handler) CommitHandler() commits.CommitHandler {
	return commits.CommitHandlerFunc(func(params commits.CommitParams, user *models.User) middleware.Responder {
		err := a.authorize(user, permissions.ManageRepos, repoArn(params.RepositoryID))
		if err != nil {
			return commits.NewCommitUnauthorized().WithPayload(responseErrorFrom(err))
		}
		commit, err := a.meta.Commit(params.RepositoryID, params.BranchID, *params.Commit.Message, user.ID, params.Commit.Metadata)
		if err != nil {
			return commits.NewCommitDefault(http.StatusInternalServerError).WithPayload(responseErrorFrom(err))
		}
		return commits.NewCommitCreated().WithPayload(&models.Commit{
			Committer:    commit.GetCommitter(),
			CreationDate: commit.GetTimestamp(),
			ID:           ident.Hash(commit),
			Message:      commit.GetMessage(),
			Metadata:     commit.GetMetadata(),
			Parents:      commit.GetParents(),
		})
	})
}

func testBucket(adapter block.Adapter, bucketName string) error {
	const (
		dummyKey  = "dummy"
		dummyData = "this is dummy data - created by lakefs in order to check accessibility "
	)

	err := adapter.Put(bucketName, dummyKey, bytes.NewReader([]byte(dummyData)))
	if err != nil {
		return err
	}

	_, err = adapter.Get(bucketName, dummyKey)
	if err != nil {
		return err
	}

	return nil
}

func (a *Handler) CreateRepositoryHandler() repositories.CreateRepositoryHandler {
	return repositories.CreateRepositoryHandlerFunc(func(params repositories.CreateRepositoryParams, user *models.User) middleware.Responder {
		err := a.authorize(user, permissions.ManageRepos, repoArn("*"))
		if err != nil {
			return repositories.NewCreateRepositoryUnauthorized().WithPayload(responseErrorFrom(err))
		}

		err = testBucket(a.blockAdapter, swag.StringValue(params.Repository.BucketName))
		if err != nil {
			return repositories.NewCreateRepositoryBadRequest().
				WithPayload(responseError("error creating repository: could not access bucket"))
		}
		err = a.meta.CreateRepo(swag.StringValue(params.Repository.ID), swag.StringValue(params.Repository.BucketName), params.Repository.DefaultBranch)
		if err != nil {
			return repositories.NewGetRepositoryDefault(http.StatusInternalServerError).
				WithPayload(responseError("error creating repository"))
		}

		repo, err := a.meta.GetRepo(swag.StringValue(params.Repository.ID))
		if err != nil {
			return repositories.NewGetRepositoryDefault(http.StatusInternalServerError).
				WithPayload(responseError("error creating repository"))
		}

		return repositories.NewCreateRepositoryCreated().WithPayload(&models.Repository{
			BucketName:    repo.GetBucketName(),
			CreationDate:  repo.GetCreationDate(),
			DefaultBranch: repo.GetDefaultBranch(),
			ID:            repo.GetRepoId(),
		})
	})
}

func (a *Handler) DeleteRepositoryHandler() repositories.DeleteRepositoryHandler {
	return repositories.DeleteRepositoryHandlerFunc(func(params repositories.DeleteRepositoryParams, user *models.User) middleware.Responder {
		err := a.authorize(user, permissions.ManageRepos, repoArn("*"))
		if err != nil {
			return repositories.NewDeleteRepositoryUnauthorized().WithPayload(responseErrorFrom(err))
		}

		err = a.meta.DeleteRepo(params.RepositoryID)
		if err != nil && xerrors.Is(err, db.ErrNotFound) {
			return repositories.NewDeleteRepositoryNotFound().
				WithPayload(responseError("repository not found"))
		} else if err != nil {
			return repositories.NewDeleteRepositoryDefault(http.StatusInternalServerError).
				WithPayload(responseError("error deleting repository"))
		}

		return repositories.NewDeleteRepositoryNoContent()
	})
}

func (a *Handler) ListBranchesHandler() branches.ListBranchesHandler {
	return branches.ListBranchesHandlerFunc(func(params branches.ListBranchesParams, user *models.User) middleware.Responder {
		err := a.authorize(user, permissions.ManageRepos, repoArn(params.RepositoryID))
		if err != nil {
			return branches.NewListBranchesUnauthorized().WithPayload(responseErrorFrom(err))
		}

		// amount
		after := ""
		amount := MaxResultsPerPage
		if params.Amount != nil {
			amount = swag.Int64Value(params.Amount)
		}

		// paginate after
		if params.After != nil {
			after = swag.StringValue(params.After)
		}

		res, hasMore, err := a.meta.ListBranches(params.RepositoryID, int(amount), after)
		if err != nil {
			return branches.NewListBranchesDefault(http.StatusInternalServerError).
				WithPayload(responseError("could not list branches"))
		}

		branchList := make([]*models.Refspec, len(res))
		var lastId string
		for i, branch := range res {
			branchList[i] = &models.Refspec{
				CommitID: &branch.Commit,
				ID:       &branch.Name,
			}
			lastId = branch.Name
		}
		returnValue := branches.NewListBranchesOK().WithPayload(&branches.ListBranchesOKBody{
			Pagination: &models.Pagination{
				HasMore:    swag.Bool(hasMore),
				Results:    swag.Int64(int64(len(branchList))),
				MaxPerPage: swag.Int64(MaxResultsPerPage),
			},
			Results: branchList,
		})

		if hasMore {
			returnValue.Payload.Pagination.NextOffset = lastId
		}

		return returnValue
	})
}

func (a *Handler) GetBranchHandler() branches.GetBranchHandler {
	return branches.GetBranchHandlerFunc(func(params branches.GetBranchParams, user *models.User) middleware.Responder {
		err := a.authorize(user, permissions.ManageRepos, repoArn(params.RepositoryID))
		if err != nil {
			return branches.NewGetBranchUnauthorized().WithPayload(responseErrorFrom(err))
		}

		branch, err := a.meta.GetBranch(params.RepositoryID, params.BranchID)
		if err != nil && xerrors.Is(err, db.ErrNotFound) {
			return branches.NewGetBranchNotFound().
				WithPayload(responseError("branch not found"))
		} else if err != nil {
			return branches.NewGetBranchDefault(http.StatusInternalServerError).
				WithPayload(responseError("error fetching branch"))
		}

		return branches.NewGetBranchOK().
			WithPayload(&models.Refspec{
				CommitID: swag.String(branch.GetCommit()),
				ID:       swag.String(branch.GetName()),
			})
	})
}

func (a *Handler) CreateBranchHandler() branches.CreateBranchHandler {
	return branches.CreateBranchHandlerFunc(func(params branches.CreateBranchParams, user *models.User) middleware.Responder {
		err := a.authorize(user, permissions.ManageRepos, repoArn(params.RepositoryID))
		if err != nil {
			return branches.NewCreateBranchUnauthorized().WithPayload(responseErrorFrom(err))
		}

		err = a.meta.CreateBranch(params.RepositoryID, swag.StringValue(params.Branch.ID), swag.StringValue(params.Branch.CommitID))
		if err != nil {
			return branches.NewCreateBranchDefault(http.StatusInternalServerError).WithPayload(responseErrorFrom(err))
		}

		return branches.NewCreateBranchCreated().WithPayload(params.Branch)
	})
}

func (a *Handler) DeleteBranchHandler() branches.DeleteBranchHandler {
	return branches.DeleteBranchHandlerFunc(func(params branches.DeleteBranchParams, user *models.User) middleware.Responder {
		err := a.authorize(user, permissions.ManageRepos, repoArn(params.RepositoryID))
		if err != nil {
			return branches.NewDeleteBranchUnauthorized().WithPayload(responseErrorFrom(err))
		}

		err = a.meta.DeleteBranch(params.RepositoryID, params.BranchID)
		if err != nil && xerrors.Is(err, db.ErrNotFound) {
			return branches.NewDeleteBranchNotFound().
				WithPayload(responseError("branch not found"))
		} else if err != nil {
			return branches.NewDeleteBranchDefault(http.StatusInternalServerError).
				WithPayload(responseError("error fetching branch"))
		}

		return branches.NewDeleteBranchNoContent()
	})
}
