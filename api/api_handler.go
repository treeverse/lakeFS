package api

import (
	"net/http"

	"github.com/go-openapi/runtime/middleware"
	"github.com/go-openapi/swag"
	"github.com/treeverse/lakefs/api/gen/models"
	"github.com/treeverse/lakefs/api/gen/restapi/operations"
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
	meta index.Index
	auth auth.Service
}

func NewHandler(meta index.Index, auth auth.Service) *Handler {
	return &Handler{
		meta: meta,
		auth: auth,
	}
}

// Configure attaches our API operations to a generated swagger API stub
// Adding new handlers requires also adding them here so that the generated server will use them
func (a *Handler) Configure(api *operations.LakefsAPI) {
	// Register operations here
	api.ListRepositoriesHandler = a.ListRepositoriesHandler()
	api.GetRepositoryHandler = a.GetRepoHandler()
	api.CreateRepositoryHandler = a.CreateRepositoryHandler()
	api.DeleteRepositoryHandler = a.DeleteRepositoryHandler()

	api.ListBranchesHandler = a.ListBranchesHandler()
	api.GetBranchHandler = a.GetBranchHandler()
	api.CreateBranchHandler = a.CreateBranchHandler()
	api.DeleteBranchHandler = a.DeleteBranchHandler()

	api.CommitHandler = a.CommitHandler()
	api.GetCommitHandler = a.GetCommitHandler()
}

func (a *Handler) authorize(user *models.User, perm permissions.Permission, arn string) error {
	return authorize(a.auth, user, perm, arn)
}

func (a *Handler) ListRepositoriesHandler() operations.ListRepositoriesHandler {
	return operations.ListRepositoriesHandlerFunc(func(params operations.ListRepositoriesParams, user *models.User) middleware.Responder {
		err := a.authorize(user, permissions.ManageRepos, repoArn("*"))
		if err != nil {
			return operations.NewListRepositoriesUnauthorized().WithPayload(responseErrorFrom(err))
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
			return operations.NewListRepositoriesDefault(http.StatusInternalServerError).
				WithPayload(responseError("could not list repositories"))
		}

		repoList := make([]*models.Repository, len(repos))
		var lastId string
		for i, repo := range repos {
			repoList[i] = &models.Repository{
				BucketName:    repo.GetRepoId(), // TODO: replace this with actual stored field that doesn't exist yet
				CreationDate:  repo.GetCreationDate(),
				DefaultBranch: repo.GetDefaultBranch(),
				ID:            repo.GetRepoId(),
			}
			lastId = repo.GetRepoId()
		}
		returnValue := operations.NewListRepositoriesOK().WithPayload(&operations.ListRepositoriesOKBody{
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

func (a *Handler) GetRepoHandler() operations.GetRepositoryHandler {
	return operations.GetRepositoryHandlerFunc(func(params operations.GetRepositoryParams, user *models.User) middleware.Responder {
		err := a.authorize(user, permissions.ManageRepos, repoArn("*"))
		if err != nil {
			return operations.NewGetRepositoryUnauthorized().WithPayload(responseErrorFrom(err))
		}

		repo, err := a.meta.GetRepo(params.RepositoryID)
		if err != nil && xerrors.Is(err, db.ErrNotFound) {
			return operations.NewGetRepositoryNotFound().
				WithPayload(responseError("repository not found"))
		} else if err != nil {
			return operations.NewGetRepositoryDefault(http.StatusInternalServerError).
				WithPayload(responseError("error fetching repository"))
		}

		return operations.NewGetRepositoryOK().
			WithPayload(&models.Repository{
				BucketName:    repo.GetRepoId(), // TODO: replace with actual bucket name
				CreationDate:  repo.GetCreationDate(),
				DefaultBranch: repo.GetDefaultBranch(),
				ID:            repo.GetRepoId(),
			})
	})
}

func (a *Handler) GetCommitHandler() operations.GetCommitHandler {
	return operations.GetCommitHandlerFunc(func(params operations.GetCommitParams, user *models.User) middleware.Responder {
		err := a.authorize(user, permissions.ManageRepos, repoArn(params.RepositoryID))
		if err != nil {
			return operations.NewGetCommitUnauthorized().WithPayload(responseErrorFrom(err))
		}
		commit, err := a.meta.GetCommit(params.RepositoryID, params.CommitID)

		if xerrors.Is(err, db.ErrNotFound) {
			return operations.NewGetCommitNotFound().WithPayload(responseError("commit not found"))
		}
		if err != nil {
			return operations.NewGetCommitDefault(http.StatusInternalServerError).WithPayload(responseErrorFrom(err))
		}
		return operations.NewGetCommitOK().WithPayload(&models.Commit{
			Committer:    commit.GetCommitter(),
			CreationDate: commit.GetTimestamp(),
			ID:           ident.Hash(commit),
			Message:      commit.GetMessage(),
			Metadata:     commit.GetMetadata(),
			Parents:      commit.GetParents(),
		})
	})
}

func (a *Handler) CommitHandler() operations.CommitHandler {
	return operations.CommitHandlerFunc(func(params operations.CommitParams, user *models.User) middleware.Responder {
		err := a.authorize(user, permissions.ManageRepos, repoArn(params.RepositoryID))
		if err != nil {
			return operations.NewCommitUnauthorized().WithPayload(responseErrorFrom(err))
		}
		commit, err := a.meta.Commit(params.RepositoryID, params.BranchID, *params.Commit.Message, user.ID, params.Commit.Metadata)
		if err != nil {
			return operations.NewCommitDefault(http.StatusInternalServerError).WithPayload(responseErrorFrom(err))
		}
		return operations.NewCommitCreated().WithPayload(&models.Commit{
			Committer:    commit.GetCommitter(),
			CreationDate: commit.GetTimestamp(),
			ID:           ident.Hash(commit),
			Message:      commit.GetMessage(),
			Metadata:     commit.GetMetadata(),
			Parents:      commit.GetParents(),
		})
	})
}

func (a *Handler) CreateRepositoryHandler() operations.CreateRepositoryHandler {
	return operations.CreateRepositoryHandlerFunc(func(params operations.CreateRepositoryParams, user *models.User) middleware.Responder {
		err := a.authorize(user, permissions.ManageRepos, repoArn("*"))
		if err != nil {
			return operations.NewCreateRepositoryUnauthorized().WithPayload(responseErrorFrom(err))
		}

		err = a.meta.CreateRepo(swag.StringValue(params.Repository.ID), params.Repository.DefaultBranch)
		if err != nil {
			return operations.NewGetRepositoryDefault(http.StatusInternalServerError).
				WithPayload(responseError("error creating repository"))
		}

		repo, err := a.meta.GetRepo(swag.StringValue(params.Repository.ID))
		if err != nil {
			return operations.NewGetRepositoryDefault(http.StatusInternalServerError).
				WithPayload(responseError("error creating repository"))
		}

		return operations.NewCreateRepositoryCreated().WithPayload(&models.Repository{
			BucketName:    repo.GetRepoId(),
			CreationDate:  repo.GetCreationDate(),
			DefaultBranch: repo.GetDefaultBranch(),
			ID:            repo.GetRepoId(),
		})
	})
}

func (a *Handler) DeleteRepositoryHandler() operations.DeleteRepositoryHandler {
	return operations.DeleteRepositoryHandlerFunc(func(params operations.DeleteRepositoryParams, user *models.User) middleware.Responder {
		err := a.authorize(user, permissions.ManageRepos, repoArn("*"))
		if err != nil {
			return operations.NewDeleteRepositoryUnauthorized().WithPayload(responseErrorFrom(err))
		}

		err = a.meta.DeleteRepo(params.RepositoryID)
		if err != nil && xerrors.Is(err, db.ErrNotFound) {
			return operations.NewDeleteRepositoryNotFound().
				WithPayload(responseError("repository not found"))
		} else if err != nil {
			return operations.NewDeleteRepositoryDefault(http.StatusInternalServerError).
				WithPayload(responseError("error deleting repository"))
		}

		return operations.NewDeleteRepositoryNoContent()
	})
}

func (a *Handler) ListBranchesHandler() operations.ListBranchesHandler {
	return operations.ListBranchesHandlerFunc(func(params operations.ListBranchesParams, user *models.User) middleware.Responder {
		err := a.authorize(user, permissions.ManageRepos, repoArn(params.RepositoryID))
		if err != nil {
			return operations.NewListBranchesUnauthorized().WithPayload(responseErrorFrom(err))
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

		branches, hasMore, err := a.meta.ListBranches(params.RepositoryID, int(amount), after)
		if err != nil {
			return operations.NewListBranchesDefault(http.StatusInternalServerError).
				WithPayload(responseError("could not list branches"))
		}

		branchList := make([]*models.Refspec, len(branches))
		var lastId string
		for i, branch := range branches {
			branchList[i] = &models.Refspec{
				CommitID: &branch.Commit,
				ID:       &branch.Name,
			}
			lastId = branch.Name
		}
		returnValue := operations.NewListBranchesOK().WithPayload(&operations.ListBranchesOKBody{
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

func (a *Handler) GetBranchHandler() operations.GetBranchHandler {
	return operations.GetBranchHandlerFunc(func(params operations.GetBranchParams, user *models.User) middleware.Responder {
		err := a.authorize(user, permissions.ManageRepos, repoArn(params.RepositoryID))
		if err != nil {
			return operations.NewGetBranchUnauthorized().WithPayload(responseErrorFrom(err))
		}

		branch, err := a.meta.GetBranch(params.RepositoryID, params.BranchID)
		if err != nil && xerrors.Is(err, db.ErrNotFound) {
			return operations.NewGetBranchNotFound().
				WithPayload(responseError("branch not found"))
		} else if err != nil {
			return operations.NewGetBranchDefault(http.StatusInternalServerError).
				WithPayload(responseError("error fetching branch"))
		}

		return operations.NewGetBranchOK().
			WithPayload(&models.Refspec{
				CommitID: swag.String(branch.GetCommit()),
				ID:       swag.String(branch.GetName()),
			})
	})
}

func (a *Handler) CreateBranchHandler() operations.CreateBranchHandler {
	return operations.CreateBranchHandlerFunc(func(params operations.CreateBranchParams, user *models.User) middleware.Responder {
		err := a.authorize(user, permissions.ManageRepos, repoArn(params.RepositoryID))
		if err != nil {
			return operations.NewCreateBranchUnauthorized().WithPayload(responseErrorFrom(err))
		}

		err = a.meta.CreateBranch(params.RepositoryID, swag.StringValue(params.Branch.ID), swag.StringValue(params.Branch.CommitID))
		if err != nil {
			return operations.NewCreateBranchDefault(http.StatusInternalServerError).WithPayload(responseErrorFrom(err))
		}

		return operations.NewCreateBranchCreated().WithPayload(params.Branch)
	})
}

func (a *Handler) DeleteBranchHandler() operations.DeleteBranchHandler {
	return operations.DeleteBranchHandlerFunc(func(params operations.DeleteBranchParams, user *models.User) middleware.Responder {
		err := a.authorize(user, permissions.ManageRepos, repoArn(params.RepositoryID))
		if err != nil {
			return operations.NewDeleteBranchUnauthorized().WithPayload(responseErrorFrom(err))
		}

		err = a.meta.DeleteBranch(params.RepositoryID, params.BranchID)
		if err != nil && xerrors.Is(err, db.ErrNotFound) {
			return operations.NewDeleteBranchNotFound().
				WithPayload(responseError("branch not found"))
		} else if err != nil {
			return operations.NewDeleteBranchDefault(http.StatusInternalServerError).
				WithPayload(responseError("error fetching branch"))
		}

		return operations.NewDeleteRepositoryNoContent()
	})
}
