package api

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	authmodel "github.com/treeverse/lakefs/auth/model"
	"github.com/treeverse/lakefs/logging"

	authentication "github.com/treeverse/lakefs/api/gen/restapi/operations/auth"

	"github.com/go-openapi/runtime"
	"github.com/go-openapi/runtime/middleware"
	"github.com/go-openapi/swag"
	"github.com/treeverse/lakefs/api/gen/models"
	"github.com/treeverse/lakefs/api/gen/restapi/operations"
	"github.com/treeverse/lakefs/api/gen/restapi/operations/branches"
	"github.com/treeverse/lakefs/api/gen/restapi/operations/commits"
	"github.com/treeverse/lakefs/api/gen/restapi/operations/objects"
	"github.com/treeverse/lakefs/api/gen/restapi/operations/refs"
	"github.com/treeverse/lakefs/api/gen/restapi/operations/repositories"
	"github.com/treeverse/lakefs/auth"
	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/httputil"
	"github.com/treeverse/lakefs/ident"
	"github.com/treeverse/lakefs/index"
	indexerrors "github.com/treeverse/lakefs/index/errors"
	"github.com/treeverse/lakefs/index/model"
	pth "github.com/treeverse/lakefs/index/path"
	"github.com/treeverse/lakefs/permissions"
	"github.com/treeverse/lakefs/stats"
	"github.com/treeverse/lakefs/upload"
)

const (
	// Maximum amount of results returned for paginated queries to the API
	MaxResultsPerPage int64 = 1000
)

type Dependencies struct {
	Index        index.Index
	Auth         auth.Service
	BlockAdapter block.Adapter
	Stats        stats.Collector
	ctx          context.Context
	logger       logging.Logger
}

func (d *Dependencies) WithContext(ctx context.Context) *Dependencies {
	return &Dependencies{
		Index:        d.Index.WithContext(ctx),
		Auth:         d.Auth, // TODO: pass context
		BlockAdapter: d.BlockAdapter.WithContext(ctx),
		Stats:        d.Stats,
		ctx:          ctx,
		logger:       d.logger.WithContext(ctx),
	}
}

func (d *Dependencies) LogAction(action string) {
	logging.FromContext(d.ctx).
		WithField("action", action).
		WithField("message_type", "action").
		Debug("performing API action")
	d.Stats.CollectEvent("api_server", action)
}

type Controller struct {
	deps *Dependencies
}

func NewController(meta index.Index, auth auth.Service, blockAdapter block.Adapter, stats stats.Collector, logger logging.Logger) *Controller {
	return &Controller{
		deps: &Dependencies{
			Index:        meta,
			Auth:         auth,
			BlockAdapter: blockAdapter,
			Stats:        stats,
			ctx:          context.Background(),
			logger:       logger,
		},
	}
}

// Configure attaches our API operations to a generated swagger API stub
// Adding new handlers requires also adding them here so that the generated server will use them
func (c *Controller) Configure(api *operations.LakefsAPI) {

	// Register operations here
	api.AuthGetCurrentUserHandler = c.GetCurrentUserHandler()
	api.AuthListUsersHandler = c.ListUsersHandler()
	api.AuthGetUserHandler = c.GetUserHandler()
	api.AuthCreateUserHandler = c.CreateUserHandler()
	api.AuthDeleteUserHandler = c.DeleteUserHandler()
	api.AuthGetGroupHandler = c.GetGroupHandler()
	api.AuthListGroupsHandler = c.ListGroupsHandler()
	api.AuthCreateGroupHandler = c.CreateGroupHandler()
	api.AuthDeleteGroupHandler = c.DeleteGroupHandler()
	api.AuthListPoliciesHandler = c.ListPoliciesHandler()
	api.AuthCreatePolicyHandler = c.CreatePolicyHandler()
	api.AuthGetPolicyHandler = c.GetPolicyHandler()
	api.AuthDeletePolicyHandler = c.DeletePolicyHandler()
	api.AuthUpdatePolicyHandler = c.UpdatePolicyHandler()
	api.AuthListGroupMembersHandler = c.ListGroupMembersHandler()
	api.AuthAddGroupMembershipHandler = c.AddGroupMembershipHandler()
	api.AuthDeleteGroupMembershipHandler = c.DeleteGroupMembershipHandler()
	api.AuthListUserCredentialsHandler = c.ListUserCredentialsHandler()
	api.AuthCreateCredentialsHandler = c.CreateCredentialsHandler()
	api.AuthDeleteCredentialsHandler = c.DeleteCredentialsHandler()
	api.AuthGetCredentialsHandler = c.GetCredentialsHandler()
	api.AuthListUserGroupsHandler = c.ListUserGroupsHandler()
	api.AuthListUserPoliciesHandler = c.ListUserPoliciesHandler()
	api.AuthAttachPolicyToUserHandler = c.AttachPolicyToUserHandler()
	api.AuthDetachPolicyFromUserHandler = c.DetachPolicyFromUserHandler()
	api.AuthListGroupPoliciesHandler = c.ListGroupPoliciesHandler()
	api.AuthAttachPolicyToGroupHandler = c.AttachPolicyToGroupHandler()
	api.AuthDetachPolicyFromGroupHandler = c.DetachPolicyFromGroupHandler()

	api.RepositoriesListRepositoriesHandler = c.ListRepositoriesHandler()
	api.RepositoriesGetRepositoryHandler = c.GetRepoHandler()
	api.RepositoriesCreateRepositoryHandler = c.CreateRepositoryHandler()
	api.RepositoriesDeleteRepositoryHandler = c.DeleteRepositoryHandler()

	api.BranchesListBranchesHandler = c.ListBranchesHandler()
	api.BranchesGetBranchHandler = c.GetBranchHandler()
	api.BranchesCreateBranchHandler = c.CreateBranchHandler()
	api.BranchesDeleteBranchHandler = c.DeleteBranchHandler()
	api.BranchesRevertBranchHandler = c.RevertBranchHandler()

	api.CommitsCommitHandler = c.CommitHandler()
	api.CommitsGetCommitHandler = c.GetCommitHandler()
	api.CommitsGetBranchCommitLogHandler = c.CommitsGetBranchCommitLogHandler()

	api.RefsDiffRefsHandler = c.RefsDiffRefsHandler()
	api.BranchesDiffBranchHandler = c.BranchesDiffBranchHandler()
	api.RefsMergeIntoBranchHandler = c.MergeMergeIntoBranchHandler()

	api.ObjectsStatObjectHandler = c.ObjectsStatObjectHandler()
	api.ObjectsGetUnderlyingPropertiesHandler = c.ObjectsGetUnderlyingPropertiesHandler()
	api.ObjectsListObjectsHandler = c.ObjectsListObjectsHandler()
	api.ObjectsGetObjectHandler = c.ObjectsGetObjectHandler()
	api.ObjectsUploadObjectHandler = c.ObjectsUploadObjectHandler()
	api.ObjectsDeleteObjectHandler = c.ObjectsDeleteObjectHandler()
}

func (c *Controller) setupRequest(user *models.User, r *http.Request, permissions []permissions.Permission) (*Dependencies, error) {
	// add user to context
	ctx := logging.AddFields(r.Context(), logging.Fields{"user": user.ID})
	ctx = context.WithValue(ctx, "user", user)
	deps := c.deps.WithContext(ctx)
	return deps, authorize(deps.Auth, user, permissions)
}

func createPaginator(nextToken string, amountResults int) *models.Pagination {
	return &models.Pagination{
		HasMore:    swag.Bool(nextToken != ""),
		MaxPerPage: swag.Int64(MaxResultsPerPage),
		NextOffset: nextToken,
		Results:    swag.Int64(int64(amountResults)),
	}
}

func pageAmount(i *int64) int {
	inti := int(swag.Int64Value(i))
	if inti > int(MaxResultsPerPage) {
		return int(MaxResultsPerPage)
	}
	if inti <= 0 {
		return 100
	}
	return inti
}

func (c *Controller) GetCurrentUserHandler() authentication.GetCurrentUserHandler {
	return authentication.GetCurrentUserHandlerFunc(func(params authentication.GetCurrentUserParams, user *models.User) middleware.Responder {
		return authentication.NewGetCurrentUserOK().WithPayload(&authentication.GetCurrentUserOKBody{
			User: user,
		})
	})
}

func (c *Controller) ListRepositoriesHandler() repositories.ListRepositoriesHandler {
	return repositories.ListRepositoriesHandlerFunc(func(params repositories.ListRepositoriesParams, user *models.User) middleware.Responder {
		deps, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.ListRepositoriesAction,
				Resource: permissions.All,
			},
		})

		if err != nil {
			return repositories.NewListRepositoriesUnauthorized().WithPayload(responseErrorFrom(err))
		}
		deps.LogAction("list_repos")

		after, amount := getPaginationParams(params.After, params.Amount)

		repos, hasMore, err := deps.Index.ListRepos(amount, after)
		if err != nil {
			return repositories.NewListRepositoriesDefault(http.StatusInternalServerError).
				WithPayload(responseError("error listing repositories: %s", err))
		}

		repoList := make([]*models.Repository, len(repos))
		var lastID string
		for i, repo := range repos {
			repoList[i] = &models.Repository{
				StorageNamespace: repo.StorageNamespace,
				CreationDate:     repo.CreationDate.Unix(),
				DefaultBranch:    repo.DefaultBranch,
				ID:               repo.Id,
			}
			lastID = repo.Id
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
			returnValue.Payload.Pagination.NextOffset = lastID
		}

		return returnValue
	})
}

func getPaginationParams(swagAfter *string, swagAmount *int64) (string, int) {
	// amount
	after := ""
	amount := MaxResultsPerPage
	if swagAmount != nil {
		amount = swag.Int64Value(swagAmount)
	}

	// paginate after
	if swagAfter != nil {
		after = swag.StringValue(swagAfter)
	}
	return after, int(amount)
}

func (c *Controller) GetRepoHandler() repositories.GetRepositoryHandler {
	return repositories.GetRepositoryHandlerFunc(func(params repositories.GetRepositoryParams, user *models.User) middleware.Responder {
		deps, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.ReadRepositoryAction,
				Resource: permissions.RepoArn(params.RepositoryID),
			},
		})
		if err != nil {
			return repositories.NewGetRepositoryUnauthorized().WithPayload(responseErrorFrom(err))
		}
		deps.LogAction("get_repo")
		repo, err := deps.Index.GetRepo(params.RepositoryID)
		if errors.Is(err, db.ErrNotFound) {
			return repositories.NewGetRepositoryNotFound().
				WithPayload(responseError("repository not found"))
		}
		if err != nil {
			return repositories.NewGetRepositoryDefault(http.StatusInternalServerError).
				WithPayload(responseError("error fetching repository: %s", err))
		}

		return repositories.NewGetRepositoryOK().
			WithPayload(&models.Repository{
				StorageNamespace: repo.StorageNamespace,
				CreationDate:     repo.CreationDate.Unix(),
				DefaultBranch:    repo.DefaultBranch,
				ID:               repo.Id,
			})
	})
}

func (c *Controller) GetCommitHandler() commits.GetCommitHandler {
	return commits.GetCommitHandlerFunc(func(params commits.GetCommitParams, user *models.User) middleware.Responder {
		deps, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.ReadCommitAction,
				Resource: permissions.RepoArn(params.RepositoryID),
			},
		})
		if err != nil {
			return commits.NewGetCommitUnauthorized().WithPayload(responseErrorFrom(err))
		}
		deps.LogAction("get_commit")
		commit, err := deps.Index.GetCommit(params.RepositoryID, params.CommitID)
		if errors.Is(err, db.ErrNotFound) {
			return commits.NewGetCommitNotFound().WithPayload(responseError("commit not found"))
		}
		if err != nil {
			return commits.NewGetCommitDefault(http.StatusInternalServerError).WithPayload(responseErrorFrom(err))
		}
		return commits.NewGetCommitOK().WithPayload(&models.Commit{
			Committer:    commit.Committer,
			CreationDate: commit.CreationDate.Unix(),
			ID:           commit.Address,
			Message:      commit.Message,
			Metadata:     commit.Metadata,
			Parents:      commit.Parents,
		})
	})
}

func (c *Controller) CommitHandler() commits.CommitHandler {
	return commits.CommitHandlerFunc(func(params commits.CommitParams, user *models.User) middleware.Responder {
		deps, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.CreateCommitAction,
				Resource: permissions.BranchArn(params.RepositoryID, params.BranchID),
			},
		})
		if err != nil {
			return commits.NewCommitUnauthorized().WithPayload(responseErrorFrom(err))
		}
		deps.LogAction("create_commit")
		userModel, err := c.deps.Auth.GetUser(user.ID)
		if err != nil {
			return commits.NewCommitUnauthorized().WithPayload(responseErrorFrom(err))
		}
		committer := userModel.DisplayName

		commit, err := deps.Index.Commit(params.RepositoryID, params.BranchID, *params.Commit.Message, committer, params.Commit.Metadata)
		if err != nil {
			return commits.NewCommitDefault(http.StatusInternalServerError).WithPayload(responseErrorFrom(err))
		}
		return commits.NewCommitCreated().WithPayload(&models.Commit{
			Committer:    commit.Committer,
			CreationDate: commit.CreationDate.Unix(),
			ID:           commit.Address,
			Message:      commit.Message,
			Metadata:     commit.Metadata,
			Parents:      commit.Parents,
		})
	})
}

func (c *Controller) CommitsGetBranchCommitLogHandler() commits.GetBranchCommitLogHandler {
	return commits.GetBranchCommitLogHandlerFunc(func(params commits.GetBranchCommitLogParams, user *models.User) middleware.Responder {
		deps, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.ReadBranchAction,
				Resource: permissions.BranchArn(params.RepositoryID, params.BranchID),
			},
		})
		if err != nil {
			return commits.NewGetBranchCommitLogUnauthorized().WithPayload(responseErrorFrom(err))
		}
		deps.LogAction("get_branch")
		index := deps.Index

		// read branch
		branch, err := index.GetBranch(params.RepositoryID, params.BranchID)
		if errors.Is(err, db.ErrNotFound) {
			return commits.NewGetBranchCommitLogNotFound().WithPayload(responseErrorFrom(err))
		}
		if err != nil {
			return commits.NewGetBranchCommitLogDefault(http.StatusInternalServerError).WithPayload(responseErrorFrom(err))
		}

		after, amount := getPaginationParams(params.After, params.Amount)
		// get commit log
		commitLog, hasMore, err := index.GetCommitLog(params.RepositoryID, branch.CommitId, amount, after)
		if err != nil {
			return commits.NewGetBranchCommitLogDefault(http.StatusInternalServerError).WithPayload(responseErrorFrom(err))
		}

		serializedCommits := make([]*models.Commit, len(commitLog))
		lastId := ""
		for i, commit := range commitLog {
			serializedCommits[i] = &models.Commit{
				Committer:    commit.Committer,
				CreationDate: commit.CreationDate.Unix(),
				ID:           commit.Address,
				Message:      commit.Message,
				Metadata:     commit.Metadata,
				Parents:      commit.Parents,
			}
			lastId = commit.Address
		}

		returnValue := commits.NewGetBranchCommitLogOK().WithPayload(&commits.GetBranchCommitLogOKBody{
			Pagination: &models.Pagination{
				HasMore:    swag.Bool(hasMore),
				Results:    swag.Int64(int64(len(serializedCommits))),
				MaxPerPage: swag.Int64(MaxResultsPerPage),
			},
			Results: serializedCommits,
		})
		if hasMore {
			returnValue.Payload.Pagination.NextOffset = lastId
		}
		return returnValue
	})
}

func ensureStorageNamespaceRW(adapter block.Adapter, storageNamespace string) error {
	const (
		dummyKey  = "dummy"
		dummyData = "this is dummy data - created by lakefs in order to check accessibility "
	)

	err := adapter.Put(block.ObjectPointer{StorageNamespace: storageNamespace, Identifier: dummyKey}, int64(len(dummyData)), bytes.NewReader([]byte(dummyData)), block.PutOpts{})
	if err != nil {
		return err
	}

	_, err = adapter.Get(block.ObjectPointer{StorageNamespace: storageNamespace, Identifier: dummyKey})
	if err != nil {
		return err
	}

	return nil
}

func (c *Controller) CreateRepositoryHandler() repositories.CreateRepositoryHandler {
	return repositories.CreateRepositoryHandlerFunc(func(params repositories.CreateRepositoryParams, user *models.User) middleware.Responder {
		deps, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.CreateRepositoryAction,
				Resource: permissions.RepoArn(swag.StringValue(params.Repository.ID)),
			},
		})
		if err != nil {
			return repositories.NewCreateRepositoryUnauthorized().WithPayload(responseErrorFrom(err))
		}
		deps.LogAction("create_repo")

		err = ensureStorageNamespaceRW(deps.BlockAdapter, swag.StringValue(params.Repository.StorageNamespace))
		if err != nil {
			return repositories.NewCreateRepositoryBadRequest().
				WithPayload(responseError("error creating repository: could not access storage namespace"))
		}
		err = deps.Index.CreateRepo(swag.StringValue(params.Repository.ID), swag.StringValue(params.Repository.StorageNamespace), params.Repository.DefaultBranch)
		if err != nil {
			return repositories.NewGetRepositoryDefault(http.StatusInternalServerError).
				WithPayload(responseError(fmt.Sprintf("error creating repository: %s", err)))
		}

		repo, err := deps.Index.GetRepo(swag.StringValue(params.Repository.ID))
		if err != nil {
			return repositories.NewGetRepositoryDefault(http.StatusInternalServerError).
				WithPayload(responseError(fmt.Sprintf("error creating repository: %s", err)))
		}

		return repositories.NewCreateRepositoryCreated().WithPayload(&models.Repository{
			StorageNamespace: repo.StorageNamespace,
			CreationDate:     repo.CreationDate.Unix(),
			DefaultBranch:    repo.DefaultBranch,
			ID:               repo.Id,
		})
	})
}

func (c *Controller) DeleteRepositoryHandler() repositories.DeleteRepositoryHandler {
	return repositories.DeleteRepositoryHandlerFunc(func(params repositories.DeleteRepositoryParams, user *models.User) middleware.Responder {
		deps, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.DeleteRepositoryAction,
				Resource: permissions.RepoArn(params.RepositoryID),
			},
		})
		if err != nil {
			return repositories.NewDeleteRepositoryUnauthorized().WithPayload(responseErrorFrom(err))
		}
		deps.LogAction("delete_repo")
		index := deps.Index
		err = index.DeleteRepo(params.RepositoryID)
		if errors.Is(err, db.ErrNotFound) {
			return repositories.NewDeleteRepositoryNotFound().
				WithPayload(responseError("repository not found"))
		}
		if err != nil {
			return repositories.NewDeleteRepositoryDefault(http.StatusInternalServerError).
				WithPayload(responseError("error deleting repository"))
		}

		return repositories.NewDeleteRepositoryNoContent()
	})
}

func (c *Controller) ListBranchesHandler() branches.ListBranchesHandler {
	return branches.ListBranchesHandlerFunc(func(params branches.ListBranchesParams, user *models.User) middleware.Responder {
		deps, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.ListBranchesAction,
				Resource: permissions.RepoArn(params.RepositoryID),
			},
		})
		if err != nil {
			return branches.NewListBranchesUnauthorized().WithPayload(responseErrorFrom(err))
		}
		deps.LogAction("list_branches")
		index := deps.Index

		after, amount := getPaginationParams(params.After, params.Amount)

		res, hasMore, err := index.ListBranchesByPrefix(params.RepositoryID, "", amount, after)
		if err != nil {
			return branches.NewListBranchesDefault(http.StatusInternalServerError).
				WithPayload(responseError("could not list branches: %s", err))
		}

		branchList := make([]*models.Ref, len(res))
		var lastId string
		for i, branch := range res {
			branchList[i] = &models.Ref{
				CommitID: &branch.CommitId,
				ID:       &branch.Id,
			}
			lastId = branch.Id
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

func (c *Controller) GetBranchHandler() branches.GetBranchHandler {
	return branches.GetBranchHandlerFunc(func(params branches.GetBranchParams, user *models.User) middleware.Responder {
		deps, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.ReadBranchAction,
				Resource: permissions.BranchArn(params.RepositoryID, params.BranchID),
			},
		})
		if err != nil {
			return branches.NewGetBranchUnauthorized().WithPayload(responseErrorFrom(err))
		}
		deps.LogAction("get_branch")
		index := deps.Index
		branch, err := index.GetBranch(params.RepositoryID, params.BranchID)
		if errors.Is(err, db.ErrNotFound) {
			return branches.NewGetBranchNotFound().
				WithPayload(responseError("branch not found"))
		}
		if err != nil {
			return branches.NewGetBranchDefault(http.StatusInternalServerError).
				WithPayload(responseError("error fetching branch: %s", err))
		}

		return branches.NewGetBranchOK().
			WithPayload(&models.Ref{
				CommitID: swag.String(branch.CommitId),
				ID:       swag.String(branch.Id),
			})
	})
}

func (c *Controller) CreateBranchHandler() branches.CreateBranchHandler {
	return branches.CreateBranchHandlerFunc(func(params branches.CreateBranchParams, user *models.User) middleware.Responder {
		deps, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.CreateBranchAction,
				Resource: permissions.BranchArn(params.RepositoryID, swag.StringValue(params.Branch.ID)),
			},
		})
		if err != nil {
			return branches.NewCreateBranchUnauthorized().WithPayload(responseErrorFrom(err))
		}
		deps.LogAction("create_branch")
		index := deps.Index
		branch, err := index.CreateBranch(params.RepositoryID, swag.StringValue(params.Branch.ID), swag.StringValue(params.Branch.SourceRefID))
		if err != nil {
			return branches.NewCreateBranchDefault(http.StatusInternalServerError).WithPayload(responseErrorFrom(err))
		}

		return branches.NewCreateBranchCreated().WithPayload(&models.Ref{
			CommitID: swag.String(branch.CommitId),
			ID:       swag.String(branch.Id),
		})
	})
}

func (c *Controller) DeleteBranchHandler() branches.DeleteBranchHandler {
	return branches.DeleteBranchHandlerFunc(func(params branches.DeleteBranchParams, user *models.User) middleware.Responder {
		deps, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.DeleteBranchAction,
				Resource: permissions.BranchArn(params.RepositoryID, params.BranchID),
			},
		})
		if err != nil {
			return branches.NewDeleteBranchUnauthorized().WithPayload(responseErrorFrom(err))
		}
		deps.LogAction("delete_branch")
		index := deps.Index
		err = index.DeleteBranch(params.RepositoryID, params.BranchID)
		if errors.Is(err, db.ErrNotFound) {
			return branches.NewDeleteBranchNotFound().
				WithPayload(responseError("branch not found"))
		}
		if err != nil {
			return branches.NewDeleteBranchDefault(http.StatusInternalServerError).
				WithPayload(responseError("error fetching branch: %s", err))
		}

		return branches.NewDeleteBranchNoContent()
	})
}

func (c *Controller) MergeMergeIntoBranchHandler() refs.MergeIntoBranchHandler {
	return refs.MergeIntoBranchHandlerFunc(func(params refs.MergeIntoBranchParams, user *models.User) middleware.Responder {
		deps, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.CreateCommitAction,
				Resource: permissions.BranchArn(params.RepositoryID, params.DestinationRef),
			},
		})
		if err != nil {
			return refs.NewMergeIntoBranchUnauthorized().WithPayload(responseErrorFrom(err))
		}
		deps.LogAction("merge_branches")
		userModel, err := deps.Auth.GetUser(user.ID)
		if err != nil {
			return refs.NewMergeIntoBranchUnauthorized().WithPayload(responseErrorFrom(err))
		}
		committer := userModel.DisplayName
		mergeOperations, err := deps.Index.Merge(params.RepositoryID, params.SourceRef, params.DestinationRef, committer)
		mergeResult := make([]*models.MergeResult, len(mergeOperations))

		if err == nil || err == indexerrors.ErrMergeConflict {
			for i, d := range mergeOperations {
				tmp := serializeDiff(d)
				mergeResult[i] = new(models.MergeResult)
				mergeResult[i].Path = tmp.Path
				mergeResult[i].Type = tmp.Type
				mergeResult[i].Direction = tmp.Direction
				mergeResult[i].PathType = tmp.PathType
			}
		}
		switch err {
		case nil:
			pl := new(refs.MergeIntoBranchOKBody)
			pl.Results = mergeResult
			return refs.NewMergeIntoBranchOK().WithPayload(pl)
		case indexerrors.ErrNoMergeBase:
			return refs.NewMergeIntoBranchDefault(http.StatusInternalServerError).WithPayload(responseError("branches have no common base"))
		case indexerrors.ErrDestinationNotCommitted:
			return refs.NewMergeIntoBranchDefault(http.StatusInternalServerError).WithPayload(responseError("destination branch have not committed before "))
		case indexerrors.ErrBranchNotFound:
			return refs.NewMergeIntoBranchDefault(http.StatusInternalServerError).WithPayload(responseError("c branch does not exist "))
		case indexerrors.ErrMergeConflict:

			pl := new(refs.MergeIntoBranchConflictBody)
			pl.Results = mergeResult
			return refs.NewMergeIntoBranchConflict().WithPayload(pl)
		default:
			return refs.NewMergeIntoBranchDefault(http.StatusInternalServerError).WithPayload(responseError("internal error"))

		}

	})
}

func (c *Controller) BranchesDiffBranchHandler() branches.DiffBranchHandler {
	return branches.DiffBranchHandlerFunc(func(params branches.DiffBranchParams, user *models.User) middleware.Responder {
		deps, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.ListObjectsAction,
				Resource: permissions.RepoArn(params.RepositoryID),
			},
		})
		if err != nil {
			return branches.NewDiffBranchUnauthorized().WithPayload(responseErrorFrom(err))
		}
		deps.LogAction("diff_workspace")
		index := deps.Index
		diff, err := index.DiffWorkspace(params.RepositoryID, params.BranchID)
		if err != nil {
			return branches.NewDiffBranchDefault(http.StatusInternalServerError).
				WithPayload(responseError("could not diff branch: %s", err))
		}

		results := make([]*models.Diff, len(diff))
		for i, d := range diff {
			results[i] = serializeDiff(d)
		}

		return branches.NewDiffBranchOK().WithPayload(&branches.DiffBranchOKBody{Results: results})
	})
}

func (c *Controller) RefsDiffRefsHandler() refs.DiffRefsHandler {
	return refs.DiffRefsHandlerFunc(func(params refs.DiffRefsParams, user *models.User) middleware.Responder {
		deps, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.ListObjectsAction,
				Resource: permissions.RepoArn(params.RepositoryID),
			},
		})
		if err != nil {
			return refs.NewDiffRefsUnauthorized().WithPayload(responseErrorFrom(err))
		}
		deps.LogAction("diff_refs")
		index := deps.Index
		diff, err := index.Diff(params.RepositoryID, params.LeftRef, params.RightRef)
		if err != nil {
			return refs.NewDiffRefsDefault(http.StatusInternalServerError).
				WithPayload(responseError("could not diff references: %s", err))
		}

		results := make([]*models.Diff, len(diff))
		for i, d := range diff {
			results[i] = serializeDiff(d)
		}
		return refs.NewDiffRefsOK().WithPayload(&refs.DiffRefsOKBody{Results: results})
	})
}

func (c *Controller) ObjectsStatObjectHandler() objects.StatObjectHandler {
	return objects.StatObjectHandlerFunc(func(params objects.StatObjectParams, user *models.User) middleware.Responder {
		deps, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.ReadObjectAction,
				Resource: permissions.ObjectArn(params.RepositoryID, params.Path),
			},
		})
		if err != nil {
			return objects.NewStatObjectUnauthorized().WithPayload(responseErrorFrom(err))
		}
		deps.LogAction("stat_object")
		idx := deps.Index

		// read metadata
		entry, err := idx.ReadEntryObject(params.RepositoryID, params.Ref, params.Path, swag.BoolValue(params.ReadUncommitted))
		if errors.Is(err, db.ErrNotFound) {
			return objects.NewStatObjectNotFound().WithPayload(responseError("resource not found"))
		}
		if err != nil {
			return objects.NewStatObjectDefault(http.StatusInternalServerError).WithPayload(responseErrorFrom(err))
		}

		// serialize entry
		return objects.NewStatObjectOK().WithPayload(&models.ObjectStats{
			Checksum:  entry.Checksum,
			Mtime:     entry.CreationDate.Unix(),
			Path:      params.Path,
			PathType:  models.ObjectStatsPathTypeOBJECT,
			SizeBytes: entry.Size,
		})
	})
}

func (c *Controller) ObjectsGetUnderlyingPropertiesHandler() objects.GetUnderlyingPropertiesHandler {
	return objects.GetUnderlyingPropertiesHandlerFunc(func(params objects.GetUnderlyingPropertiesParams, user *models.User) middleware.Responder {
		deps, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.ReadObjectAction,
				Resource: permissions.ObjectArn(params.RepositoryID, params.Path),
			},
		})
		if err != nil {
			return objects.NewGetUnderlyingPropertiesUnauthorized().WithPayload(responseErrorFrom(err))
		}
		deps.LogAction("object_underlying_properties")
		idx := deps.Index

		// read repo
		repo, err := idx.GetRepo(params.RepositoryID)
		if errors.Is(err, db.ErrNotFound) {
			return objects.NewGetObjectNotFound().WithPayload(responseError("resource not found"))
		}
		if err != nil {
			return objects.NewGetObjectDefault(http.StatusInternalServerError).WithPayload(responseErrorFrom(err))
		}

		obj, err := idx.ReadObject(params.RepositoryID, params.Ref, params.Path, swag.BoolValue(params.ReadUncommitted))
		if errors.Is(err, db.ErrNotFound) {
			return objects.NewGetUnderlyingPropertiesNotFound().WithPayload(responseError("resource not found"))
		}
		if err != nil {
			return objects.NewGetUnderlyingPropertiesDefault(http.StatusInternalServerError).WithPayload(responseErrorFrom(err))
		}

		// read object properties from underlying storage
		properties, err := c.deps.BlockAdapter.GetProperties(block.ObjectPointer{StorageNamespace: repo.StorageNamespace, Identifier: obj.PhysicalAddress})
		if err != nil {
			return objects.NewGetUnderlyingPropertiesDefault(http.StatusInternalServerError).WithPayload(responseErrorFrom(err))
		}

		// serialize properties
		return objects.NewGetUnderlyingPropertiesOK().WithPayload(&models.UnderlyingObjectProperties{
			StorageClass: properties.StorageClass,
		})
	})
}

func (c *Controller) ObjectsGetObjectHandler() objects.GetObjectHandler {
	return objects.GetObjectHandlerFunc(func(params objects.GetObjectParams, user *models.User) middleware.Responder {
		deps, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.ReadObjectAction,
				Resource: permissions.ObjectArn(params.RepositoryID, params.Path),
			},
		})
		if err != nil {
			return objects.NewGetObjectUnauthorized().WithPayload(responseErrorFrom(err))
		}
		deps.LogAction("get_object")
		idx := deps.Index

		// read repo
		repo, err := idx.GetRepo(params.RepositoryID)
		if errors.Is(err, db.ErrNotFound) {
			return objects.NewGetObjectNotFound().WithPayload(responseError("resource not found"))
		}
		if err != nil {
			return objects.NewGetObjectDefault(http.StatusInternalServerError).WithPayload(responseErrorFrom(err))
		}

		// read the FS entry
		entry, err := idx.ReadEntryObject(params.RepositoryID, params.Ref, params.Path, swag.BoolValue(params.ReadUncommitted))
		if errors.Is(err, db.ErrNotFound) {
			return objects.NewGetObjectNotFound().WithPayload(responseError("resource not found"))
		}
		if err != nil {
			return objects.NewGetObjectDefault(http.StatusInternalServerError).WithPayload(responseErrorFrom(err))
		}
		// setup response
		res := objects.NewGetObjectOK()
		res.ETag = httputil.ETag(entry.Checksum)
		res.LastModified = httputil.HeaderTimestamp(entry.CreationDate)
		res.ContentDisposition = fmt.Sprintf("filename=\"%s\"", entry.GetName())

		// get object for its blocks
		obj, err := idx.ReadObject(params.RepositoryID, params.Ref, params.Path, swag.BoolValue(params.ReadUncommitted))
		if err != nil {
			return objects.NewGetObjectDefault(http.StatusInternalServerError).WithPayload(responseErrorFrom(err))
		}

		// build c response as c multi-reader
		res.ContentLength = obj.Size
		reader, err := deps.BlockAdapter.Get(block.ObjectPointer{StorageNamespace: repo.StorageNamespace, Identifier: obj.PhysicalAddress})
		if err != nil {
			return objects.NewGetObjectDefault(http.StatusInternalServerError).WithPayload(responseErrorFrom(err))
		}

		// done
		res.Payload = reader
		return res
	})
}

func (c *Controller) ObjectsListObjectsHandler() objects.ListObjectsHandler {
	return objects.ListObjectsHandlerFunc(func(params objects.ListObjectsParams, user *models.User) middleware.Responder {
		deps, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.ListObjectsAction,
				Resource: permissions.RepoArn(params.RepositoryID),
			},
		})
		if err != nil {
			return objects.NewListObjectsUnauthorized().WithPayload(responseErrorFrom(err))
		}
		deps.LogAction("list_objects")
		idx := deps.Index

		after, amount := getPaginationParams(params.After, params.Amount)

		res, hasMore, err := idx.ListObjectsByPrefix(
			params.RepositoryID,
			params.Ref,
			swag.StringValue(params.Tree),
			after,
			amount,
			false,
			swag.BoolValue(params.ReadUncommitted),
		)
		if errors.Is(err, db.ErrNotFound) {
			return objects.NewListObjectsNotFound().WithPayload(responseError("could not find requested path"))
		}
		if err != nil {
			return objects.NewListObjectsDefault(http.StatusInternalServerError).
				WithPayload(responseError("error while listing objects: %s", err))
		}

		objList := make([]*models.ObjectStats, len(res))
		var lastId string
		for i, entry := range res {
			typ := models.ObjectStatsPathTypeTREE
			if entry.GetType() == model.EntryTypeObject {
				typ = models.ObjectStatsPathTypeOBJECT
			}
			mtime := entry.CreationDate.Unix()
			if entry.CreationDate.IsZero() {
				mtime = 0
			}
			objList[i] = &models.ObjectStats{
				Checksum:  entry.Checksum,
				Mtime:     mtime,
				Path:      entry.GetName(),
				PathType:  typ,
				SizeBytes: entry.Size,
			}
			lastId = entry.GetName()
		}
		returnValue := objects.NewListObjectsOK().WithPayload(&objects.ListObjectsOKBody{
			Pagination: &models.Pagination{
				HasMore:    swag.Bool(hasMore),
				Results:    swag.Int64(int64(len(objList))),
				MaxPerPage: swag.Int64(MaxResultsPerPage),
			},
			Results: objList,
		})

		if hasMore {
			returnValue.Payload.Pagination.NextOffset = lastId
		}
		return returnValue
	})
}

func (c *Controller) ObjectsUploadObjectHandler() objects.UploadObjectHandler {
	return objects.UploadObjectHandlerFunc(func(params objects.UploadObjectParams, user *models.User) middleware.Responder {
		deps, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.WriteObjectAction,
				Resource: permissions.ObjectArn(params.RepositoryID, params.Path),
			},
		})
		if err != nil {
			return objects.NewUploadObjectUnauthorized().WithPayload(responseErrorFrom(err))
		}
		deps.LogAction("put_object")
		index := deps.Index

		repo, err := index.GetRepo(params.RepositoryID)
		if errors.Is(err, db.ErrNotFound) {
			return objects.NewUploadObjectNotFound().WithPayload(responseError("resource not found"))
		}
		if err != nil {
			return objects.NewUploadObjectDefault(http.StatusInternalServerError).WithPayload(responseErrorFrom(err))
		}
		// workaround in order to extract file content-length using swagger
		file, ok := params.Content.(*runtime.File)
		if !ok {
			return objects.NewUploadObjectNotFound().WithPayload(responseError("failed extracting size from file"))
		}
		byteSize := file.Header.Size

		// read the content
		checksum, physicalAddress, size, err := upload.WriteBlob(index, repo.Id, repo.StorageNamespace, params.Content, deps.BlockAdapter, byteSize, block.PutOpts{StorageClass: params.StorageClass})
		if err != nil {
			return objects.NewUploadObjectDefault(http.StatusInternalServerError).WithPayload(responseErrorFrom(err))
		}

		// write metadata
		writeTime := time.Now()
		obj := &model.Object{
			RepositoryId:    repo.Id,
			PhysicalAddress: physicalAddress,
			Checksum:        checksum,
			Size:            size,
		}

		p := pth.New(params.Path, model.EntryTypeObject)

		entry := &model.Entry{
			RepositoryId: repo.Id,
			Name:         p.BaseName(),
			Address:      ident.Hash(obj),
			EntryType:    model.EntryTypeObject,
			CreationDate: writeTime,
			Size:         size,
			Checksum:     checksum,
			ObjectCount:  1,
		}
		err = index.WriteFile(repo.Id, params.BranchID, params.Path, entry, obj)
		if err != nil {
			return objects.NewUploadObjectDefault(http.StatusInternalServerError).WithPayload(responseErrorFrom(err))
		}
		return objects.NewUploadObjectCreated().WithPayload(&models.ObjectStats{
			Checksum:  checksum,
			Mtime:     writeTime.Unix(),
			Path:      params.Path,
			PathType:  models.ObjectStatsPathTypeOBJECT,
			SizeBytes: size,
		})
	})
}

func (c *Controller) ObjectsDeleteObjectHandler() objects.DeleteObjectHandler {
	return objects.DeleteObjectHandlerFunc(func(params objects.DeleteObjectParams, user *models.User) middleware.Responder {
		deps, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.DeleteObjectAction,
				Resource: permissions.ObjectArn(params.RepositoryID, params.Path),
			},
		})
		if err != nil {
			return objects.NewDeleteObjectUnauthorized().WithPayload(responseErrorFrom(err))
		}
		deps.LogAction("delete_object")
		index := deps.Index

		err = index.DeleteObject(params.RepositoryID, params.BranchID, params.Path)
		if errors.Is(err, db.ErrNotFound) {
			return objects.NewDeleteObjectNotFound().WithPayload(responseError("resource not found"))
		}
		if err != nil {
			return objects.NewDeleteObjectDefault(http.StatusInternalServerError).WithPayload(responseErrorFrom(err))
		}

		return objects.NewDeleteObjectNoContent()
	})
}
func (c *Controller) RevertBranchHandler() branches.RevertBranchHandler {
	return branches.RevertBranchHandlerFunc(func(params branches.RevertBranchParams, user *models.User) middleware.Responder {
		deps, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.RevertBranchAction,
				Resource: permissions.BranchArn(params.RepositoryID, params.BranchID),
			},
		})
		if err != nil {
			return branches.NewRevertBranchUnauthorized().WithPayload(responseErrorFrom(err))
		}
		deps.LogAction("revert_branch")
		index := deps.Index

		switch swag.StringValue(params.Revert.Type) {
		case models.RevertCreationTypeCOMMIT:
			err = index.RevertCommit(params.RepositoryID, params.BranchID, params.Revert.Commit)

		case models.RevertCreationTypeTREE:
			err = index.RevertPath(params.RepositoryID, params.BranchID, params.Revert.Path)

		case models.RevertCreationTypeRESET:
			err = index.ResetBranch(params.RepositoryID, params.BranchID)

		case models.RevertCreationTypeOBJECT:
			err = index.RevertObject(params.RepositoryID, params.BranchID, params.Revert.Path)
		default:
			return branches.NewRevertBranchNotFound().
				WithPayload(responseError("revert type not found"))
		}
		if errors.Is(err, db.ErrNotFound) {
			return branches.NewRevertBranchNotFound().WithPayload(responseError("branch not found"))
		}
		if err != nil {
			return branches.NewRevertBranchDefault(http.StatusInternalServerError).WithPayload(responseErrorFrom(err))
		}

		return branches.NewRevertBranchNoContent()
	})
}

func (c *Controller) CreateUserHandler() authentication.CreateUserHandler {
	return authentication.CreateUserHandlerFunc(func(params authentication.CreateUserParams, user *models.User) middleware.Responder {
		deps, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.CreateUserAction,
				Resource: permissions.UserArn(swag.StringValue(params.User.ID)),
			},
		})
		if err != nil {
			return authentication.NewCreateUserUnauthorized().
				WithPayload(responseErrorFrom(err))
		}
		u := &authmodel.User{
			CreatedAt:   time.Now(),
			DisplayName: swag.StringValue(params.User.ID),
		}
		err = deps.Auth.CreateUser(u)
		deps.LogAction("create_user")
		if err != nil {
			return authentication.NewCreateUserDefault(http.StatusInternalServerError).
				WithPayload(responseErrorFrom(err))
		}

		return authentication.NewCreateUserCreated().
			WithPayload(&models.User{
				CreationDate: u.CreatedAt.Unix(),
				ID:           u.DisplayName,
			})
	})
}

func (c *Controller) ListUsersHandler() authentication.ListUsersHandler {
	return authentication.ListUsersHandlerFunc(func(params authentication.ListUsersParams, user *models.User) middleware.Responder {
		deps, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.ListUsersAction,
				Resource: permissions.All,
			},
		})
		if err != nil {
			return authentication.NewListUsersUnauthorized().
				WithPayload(responseErrorFrom(err))
		}

		deps.LogAction("list_users")
		users, paginator, err := deps.Auth.ListUsers(&authmodel.PaginationParams{
			After:  swag.StringValue(params.After),
			Amount: pageAmount(params.Amount),
		})
		if err != nil {
			return authentication.NewListUsersDefault(http.StatusInternalServerError).
				WithPayload(responseErrorFrom(err))
		}

		response := make([]*models.User, len(users))
		for i, u := range users {
			response[i] = &models.User{
				CreationDate: u.CreatedAt.Unix(),
				ID:           u.DisplayName,
			}
		}

		return authentication.NewListUsersOK().
			WithPayload(&authentication.ListUsersOKBody{
				Pagination: createPaginator(paginator.NextPageToken, len(response)),
				Results:    response,
			})
	})
}

func (c *Controller) GetUserHandler() authentication.GetUserHandler {
	return authentication.GetUserHandlerFunc(func(params authentication.GetUserParams, user *models.User) middleware.Responder {
		deps, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.ReadUserAction,
				Resource: permissions.UserArn(params.UserID),
			},
		})
		if err != nil {
			return authentication.NewGetUserUnauthorized().
				WithPayload(responseErrorFrom(err))
		}
		deps.LogAction("get_user")
		u, err := deps.Auth.GetUser(params.UserID)
		if errors.Is(err, db.ErrNotFound) {
			return authentication.NewGetUserNotFound().
				WithPayload(responseError("user not found"))
		}
		if err != nil {
			return authentication.NewGetUserDefault(http.StatusInternalServerError).
				WithPayload(responseErrorFrom(err))
		}

		return authentication.NewGetUserOK().
			WithPayload(&models.User{
				CreationDate: u.CreatedAt.Unix(),
				ID:           u.DisplayName,
			})
	})
}

func (c *Controller) DeleteUserHandler() authentication.DeleteUserHandler {
	return authentication.DeleteUserHandlerFunc(func(params authentication.DeleteUserParams, user *models.User) middleware.Responder {
		deps, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.DeleteUserAction,
				Resource: permissions.UserArn(params.UserID),
			},
		})
		if err != nil {
			return authentication.NewDeleteUserUnauthorized().
				WithPayload(responseErrorFrom(err))
		}

		deps.LogAction("delete_user")
		err = deps.Auth.DeleteUser(params.UserID)
		if errors.Is(err, db.ErrNotFound) {
			return authentication.NewDeleteUserNotFound().
				WithPayload(responseError("user not found"))
		}
		if err != nil {
			return authentication.NewDeleteUserDefault(http.StatusInternalServerError).
				WithPayload(responseErrorFrom(err))
		}

		return authentication.NewDeleteUserNoContent()
	})
}

func (c *Controller) GetGroupHandler() authentication.GetGroupHandler {
	return authentication.GetGroupHandlerFunc(func(params authentication.GetGroupParams, user *models.User) middleware.Responder {
		deps, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.ReadGroupAction,
				Resource: permissions.GroupArn(params.GroupID),
			},
		})
		if err != nil {
			return authentication.NewGetGroupUnauthorized().
				WithPayload(responseErrorFrom(err))
		}
		deps.LogAction("get_group")
		g, err := deps.Auth.GetGroup(params.GroupID)
		if errors.Is(err, db.ErrNotFound) {
			return authentication.NewGetGroupNotFound().
				WithPayload(responseError("group not found"))
		}
		if err != nil {
			return authentication.NewGetGroupDefault(http.StatusInternalServerError).
				WithPayload(responseErrorFrom(err))
		}

		return authentication.NewGetGroupOK().
			WithPayload(&models.Group{
				CreationDate: g.CreatedAt.Unix(),
				ID:           g.DisplayName,
			})
	})
}

func (c *Controller) ListGroupsHandler() authentication.ListGroupsHandler {
	return authentication.ListGroupsHandlerFunc(func(params authentication.ListGroupsParams, user *models.User) middleware.Responder {
		deps, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.ListGroupsAction,
				Resource: permissions.All,
			},
		})
		if err != nil {
			return authentication.NewListGroupsUnauthorized().
				WithPayload(responseErrorFrom(err))
		}

		deps.LogAction("list_groups")
		groups, paginator, err := deps.Auth.ListGroups(&authmodel.PaginationParams{
			After:  swag.StringValue(params.After),
			Amount: pageAmount(params.Amount),
		})

		if err != nil {
			return authentication.NewListGroupsDefault(http.StatusInternalServerError).
				WithPayload(responseErrorFrom(err))
		}

		response := make([]*models.Group, len(groups))
		for i, g := range groups {
			response[i] = &models.Group{
				CreationDate: g.CreatedAt.Unix(),
				ID:           g.DisplayName,
			}
		}

		return authentication.NewListGroupsOK().
			WithPayload(&authentication.ListGroupsOKBody{
				Pagination: createPaginator(paginator.NextPageToken, len(response)),
				Results:    response,
			})
	})
}

func (c *Controller) CreateGroupHandler() authentication.CreateGroupHandler {
	return authentication.CreateGroupHandlerFunc(func(params authentication.CreateGroupParams, user *models.User) middleware.Responder {
		deps, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.CreateGroupAction,
				Resource: permissions.GroupArn(swag.StringValue(params.Group.ID)),
			},
		})
		if err != nil {
			return authentication.NewCreateGroupUnauthorized().
				WithPayload(responseErrorFrom(err))
		}
		g := &authmodel.Group{
			CreatedAt:   time.Now(),
			DisplayName: swag.StringValue(params.Group.ID),
		}

		deps.LogAction("create_group")
		err = deps.Auth.CreateGroup(g)
		if err != nil {
			return authentication.NewCreateGroupDefault(http.StatusInternalServerError).
				WithPayload(responseErrorFrom(err))
		}

		return authentication.NewCreateGroupCreated().
			WithPayload(&models.Group{
				CreationDate: g.CreatedAt.Unix(),
				ID:           g.DisplayName,
			})
	})
}

func (c *Controller) DeleteGroupHandler() authentication.DeleteGroupHandler {
	return authentication.DeleteGroupHandlerFunc(func(params authentication.DeleteGroupParams, user *models.User) middleware.Responder {
		deps, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.DeleteGroupAction,
				Resource: permissions.GroupArn(params.GroupID),
			},
		})
		if err != nil {
			return authentication.NewDeleteGroupUnauthorized().
				WithPayload(responseErrorFrom(err))
		}

		deps.LogAction("delete_group")
		err = deps.Auth.DeleteGroup(params.GroupID)
		if errors.Is(err, db.ErrNotFound) {
			return authentication.NewDeleteGroupNotFound().
				WithPayload(responseError("group not found"))
		}
		if err != nil {
			return authentication.NewDeleteGroupDefault(http.StatusInternalServerError).
				WithPayload(responseErrorFrom(err))
		}
		return authentication.NewDeleteGroupNoContent()
	})
}

func serializePolicy(p *authmodel.Policy) *models.Policy {
	stmts := make([]*models.Statement, len(p.Statement))
	for i, s := range p.Statement {
		stmts[i] = &models.Statement{
			Action:   s.Action,
			Effect:   swag.String(s.Effect),
			Resource: swag.String(s.Resource),
		}
	}
	return &models.Policy{
		ID:           swag.String(p.DisplayName),
		CreationDate: p.CreatedAt.Unix(),
		Statement:    stmts,
	}
}

func (c *Controller) ListPoliciesHandler() authentication.ListPoliciesHandler {
	return authentication.ListPoliciesHandlerFunc(func(params authentication.ListPoliciesParams, user *models.User) middleware.Responder {
		deps, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.ListPoliciesAction,
				Resource: permissions.All,
			},
		})
		if err != nil {
			return authentication.NewListPoliciesUnauthorized().
				WithPayload(responseErrorFrom(err))
		}

		deps.LogAction("list_policies")
		policies, paginator, err := deps.Auth.ListPolicies(&authmodel.PaginationParams{
			After:  swag.StringValue(params.After),
			Amount: pageAmount(params.Amount),
		})
		if err != nil {
			return authentication.NewListPoliciesDefault(http.StatusInternalServerError).
				WithPayload(responseErrorFrom(err))
		}

		response := make([]*models.Policy, len(policies))
		for i, p := range policies {
			response[i] = serializePolicy(p)
		}

		return authentication.NewListPoliciesOK().
			WithPayload(&authentication.ListPoliciesOKBody{
				Pagination: createPaginator(paginator.NextPageToken, len(response)),
				Results:    response,
			})
	})
}

func (c *Controller) CreatePolicyHandler() authentication.CreatePolicyHandler {
	return authentication.CreatePolicyHandlerFunc(func(params authentication.CreatePolicyParams, user *models.User) middleware.Responder {
		deps, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.CreatePolicyAction,
				Resource: permissions.PolicyArn(swag.StringValue(params.Policy.ID)),
			},
		})
		if err != nil {
			return authentication.NewCreatePolicyUnauthorized().
				WithPayload(responseErrorFrom(err))
		}

		stmts := make(authmodel.Statements, len(params.Policy.Statement))
		for i, apiStatement := range params.Policy.Statement {
			stmts[i] = authmodel.Statement{
				Effect:   swag.StringValue(apiStatement.Effect),
				Action:   apiStatement.Action,
				Resource: swag.StringValue(apiStatement.Resource),
			}
		}

		p := &authmodel.Policy{
			CreatedAt:   time.Now(),
			DisplayName: swag.StringValue(params.Policy.ID),
			Statement:   stmts,
		}

		deps.LogAction("create_policy")
		err = deps.Auth.WritePolicy(p)
		if err != nil {
			return authentication.NewCreatePolicyDefault(http.StatusInternalServerError).
				WithPayload(responseErrorFrom(err))
		}

		return authentication.NewCreatePolicyCreated().
			WithPayload(serializePolicy(p))
	})
}

func (c *Controller) GetPolicyHandler() authentication.GetPolicyHandler {
	return authentication.GetPolicyHandlerFunc(func(params authentication.GetPolicyParams, user *models.User) middleware.Responder {
		deps, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.ReadPolicyAction,
				Resource: permissions.PolicyArn(params.PolicyID),
			},
		})
		if err != nil {
			return authentication.NewGetPolicyUnauthorized().
				WithPayload(responseErrorFrom(err))
		}
		deps.LogAction("get_policy")
		p, err := deps.Auth.GetPolicy(params.PolicyID)
		if errors.Is(err, db.ErrNotFound) {
			return authentication.NewGetPolicyNotFound().
				WithPayload(responseError("policy not found"))
		}
		if err != nil {
			return authentication.NewGetPolicyDefault(http.StatusInternalServerError).
				WithPayload(responseErrorFrom(err))
		}

		return authentication.NewGetPolicyOK().
			WithPayload(serializePolicy(p))
	})
}

func (c *Controller) UpdatePolicyHandler() authentication.UpdatePolicyHandler {
	return authentication.UpdatePolicyHandlerFunc(func(params authentication.UpdatePolicyParams, user *models.User) middleware.Responder {
		deps, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.UpdatePolicyAction,
				Resource: permissions.PolicyArn(params.PolicyID),
			},
		})
		if err != nil {
			return authentication.NewUpdatePolicyUnauthorized().
				WithPayload(responseErrorFrom(err))
		}

		stmts := make(authmodel.Statements, len(params.Policy.Statement))
		for i, apiStatement := range params.Policy.Statement {
			stmts[i] = authmodel.Statement{
				Effect:   swag.StringValue(apiStatement.Effect),
				Action:   apiStatement.Action,
				Resource: swag.StringValue(apiStatement.Resource),
			}
		}

		p := &authmodel.Policy{
			CreatedAt:   time.Now(),
			DisplayName: swag.StringValue(params.Policy.ID),
			Statement:   stmts,
		}

		deps.LogAction("update_policy")
		err = deps.Auth.WritePolicy(p)
		if err != nil {
			return authentication.NewUpdatePolicyDefault(http.StatusInternalServerError).
				WithPayload(responseErrorFrom(err))
		}

		return authentication.NewUpdatePolicyOK().
			WithPayload(serializePolicy(p))
	})
}

func (c *Controller) DeletePolicyHandler() authentication.DeletePolicyHandler {
	return authentication.DeletePolicyHandlerFunc(func(params authentication.DeletePolicyParams, user *models.User) middleware.Responder {
		deps, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.DeletePolicyAction,
				Resource: permissions.PolicyArn(params.PolicyID),
			},
		})
		if err != nil {
			return authentication.NewDeletePolicyUnauthorized().
				WithPayload(responseErrorFrom(err))
		}

		deps.LogAction("delete_policy")
		err = deps.Auth.DeletePolicy(params.PolicyID)
		if errors.Is(err, db.ErrNotFound) {
			return authentication.NewDeletePolicyNotFound().
				WithPayload(responseError("policy not found"))
		}
		if err != nil {
			return authentication.NewDeletePolicyDefault(http.StatusInternalServerError).
				WithPayload(responseErrorFrom(err))
		}
		return authentication.NewDeletePolicyNoContent()
	})
}

func (c *Controller) ListGroupMembersHandler() authentication.ListGroupMembersHandler {
	return authentication.ListGroupMembersHandlerFunc(func(params authentication.ListGroupMembersParams, user *models.User) middleware.Responder {
		deps, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.ReadGroupAction,
				Resource: permissions.GroupArn(params.GroupID),
			},
		})
		if err != nil {
			return authentication.NewListGroupMembersUnauthorized().
				WithPayload(responseErrorFrom(err))
		}

		deps.LogAction("list_group_users")
		users, paginator, err := deps.Auth.ListGroupUsers(params.GroupID, &authmodel.PaginationParams{
			After:  swag.StringValue(params.After),
			Amount: pageAmount(params.Amount),
		})
		if err != nil {
			return authentication.NewListGroupMembersDefault(http.StatusInternalServerError).
				WithPayload(responseErrorFrom(err))
		}

		response := make([]*models.User, len(users))
		for i, u := range users {
			response[i] = &models.User{
				CreationDate: u.CreatedAt.Unix(),
				ID:           u.DisplayName,
			}
		}

		return authentication.NewListGroupMembersOK().
			WithPayload(&authentication.ListGroupMembersOKBody{
				Pagination: createPaginator(paginator.NextPageToken, len(response)),
				Results:    response,
			})
	})
}

func (c *Controller) AddGroupMembershipHandler() authentication.AddGroupMembershipHandler {
	return authentication.AddGroupMembershipHandlerFunc(func(params authentication.AddGroupMembershipParams, user *models.User) middleware.Responder {
		deps, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.AddGroupMemberAction,
				Resource: permissions.GroupArn(params.GroupID),
			},
		})
		if err != nil {
			return authentication.NewAddGroupMembershipUnauthorized().
				WithPayload(responseErrorFrom(err))
		}

		deps.LogAction("add_user_to_group")
		err = deps.Auth.AddUserToGroup(params.UserID, params.GroupID)
		if err != nil {
			return authentication.NewAddGroupMembershipDefault(http.StatusInternalServerError).
				WithPayload(responseErrorFrom(err))
		}

		return authentication.NewAddGroupMembershipCreated()
	})
}

func (c *Controller) DeleteGroupMembershipHandler() authentication.DeleteGroupMembershipHandler {
	return authentication.DeleteGroupMembershipHandlerFunc(func(params authentication.DeleteGroupMembershipParams, user *models.User) middleware.Responder {
		deps, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.RemoveGroupMemberAction,
				Resource: permissions.GroupArn(params.GroupID),
			},
		})
		if err != nil {
			return authentication.NewDeleteGroupMembershipUnauthorized().
				WithPayload(responseErrorFrom(err))
		}

		deps.LogAction("remove_user_from_group")
		err = deps.Auth.RemoveUserFromGroup(params.UserID, params.GroupID)
		if err != nil {
			return authentication.NewDeleteGroupMembershipDefault(http.StatusInternalServerError).
				WithPayload(responseErrorFrom(err))
		}

		return authentication.NewDeleteGroupMembershipNoContent()
	})
}

func (c *Controller) ListUserCredentialsHandler() authentication.ListUserCredentialsHandler {
	return authentication.ListUserCredentialsHandlerFunc(func(params authentication.ListUserCredentialsParams, user *models.User) middleware.Responder {
		deps, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.ListCredentialsAction,
				Resource: permissions.UserArn(params.UserID),
			},
		})
		if err != nil {
			return authentication.NewListUserCredentialsUnauthorized().
				WithPayload(responseErrorFrom(err))
		}

		deps.LogAction("list_user_credentials")
		credentials, paginator, err := deps.Auth.ListUserCredentials(params.UserID, &authmodel.PaginationParams{
			After:  swag.StringValue(params.After),
			Amount: pageAmount(params.Amount),
		})
		if err != nil {
			return authentication.NewListUserCredentialsDefault(http.StatusInternalServerError).
				WithPayload(responseErrorFrom(err))
		}

		response := make([]*models.Credentials, len(credentials))
		for i, c := range credentials {
			response[i] = &models.Credentials{
				AccessKeyID:  c.AccessKeyId,
				CreationDate: c.IssuedDate.Unix(),
			}
		}

		return authentication.NewListUserCredentialsOK().
			WithPayload(&authentication.ListUserCredentialsOKBody{
				Pagination: createPaginator(paginator.NextPageToken, len(response)),
				Results:    response,
			})
	})
}

func (c *Controller) CreateCredentialsHandler() authentication.CreateCredentialsHandler {
	return authentication.CreateCredentialsHandlerFunc(func(params authentication.CreateCredentialsParams, user *models.User) middleware.Responder {
		deps, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.CreateCredentialsAction,
				Resource: permissions.UserArn(params.UserID),
			},
		})
		if err != nil {
			return authentication.NewCreateCredentialsUnauthorized().
				WithPayload(responseErrorFrom(err))
		}

		deps.LogAction("create_credentials")
		credentials, err := deps.Auth.CreateCredentials(params.UserID)
		if err != nil {
			return authentication.NewCreateCredentialsDefault(http.StatusInternalServerError).
				WithPayload(responseErrorFrom(err))
		}

		return authentication.NewCreateCredentialsCreated().
			WithPayload(&models.CredentialsWithSecret{
				AccessKeyID:     credentials.AccessKeyId,
				AccessSecretKey: credentials.AccessSecretKey,
				CreationDate:    credentials.IssuedDate.Unix(),
			})
	})
}

func (c *Controller) DeleteCredentialsHandler() authentication.DeleteCredentialsHandler {
	return authentication.DeleteCredentialsHandlerFunc(func(params authentication.DeleteCredentialsParams, user *models.User) middleware.Responder {
		deps, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.DeleteCredentialsAction,
				Resource: permissions.UserArn(params.UserID),
			},
		})
		if err != nil {
			return authentication.NewDeleteCredentialsUnauthorized().
				WithPayload(responseErrorFrom(err))
		}

		deps.LogAction("delete_credentials")
		err = deps.Auth.DeleteCredentials(params.UserID, params.AccessKeyID)
		if errors.Is(err, db.ErrNotFound) {
			return authentication.NewDeleteCredentialsNotFound().
				WithPayload(responseError("credentials not found"))
		}
		if err != nil {
			return authentication.NewDeleteCredentialsDefault(http.StatusInternalServerError).
				WithPayload(responseErrorFrom(err))
		}

		return authentication.NewDeleteCredentialsNoContent()
	})
}

func (c *Controller) GetCredentialsHandler() authentication.GetCredentialsHandler {
	return authentication.GetCredentialsHandlerFunc(func(params authentication.GetCredentialsParams, user *models.User) middleware.Responder {
		deps, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.ReadCredentialsAction,
				Resource: permissions.UserArn(params.UserID),
			},
		})
		if err != nil {
			return authentication.NewGetCredentialsUnauthorized().
				WithPayload(responseErrorFrom(err))
		}
		deps.LogAction("get_credentials_for_user")
		credentials, err := deps.Auth.GetCredentialsForUser(params.UserID, params.AccessKeyID)
		if errors.Is(err, db.ErrNotFound) {
			return authentication.NewGetCredentialsNotFound().
				WithPayload(responseError("credentials not found"))
		}
		if err != nil {
			return authentication.NewGetCredentialsDefault(http.StatusInternalServerError).
				WithPayload(responseErrorFrom(err))
		}

		return authentication.NewGetCredentialsOK().
			WithPayload(&models.Credentials{
				AccessKeyID:  credentials.AccessKeyId,
				CreationDate: credentials.IssuedDate.Unix(),
			})
	})
}

func (c *Controller) ListUserGroupsHandler() authentication.ListUserGroupsHandler {
	return authentication.ListUserGroupsHandlerFunc(func(params authentication.ListUserGroupsParams, user *models.User) middleware.Responder {
		deps, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.ReadUserAction,
				Resource: permissions.UserArn(params.UserID),
			},
		})
		if err != nil {
			return authentication.NewListUserGroupsUnauthorized().
				WithPayload(responseErrorFrom(err))
		}

		deps.LogAction("list_user_groups")
		groups, paginator, err := deps.Auth.ListUserGroups(params.UserID, &authmodel.PaginationParams{
			After:  swag.StringValue(params.After),
			Amount: pageAmount(params.Amount),
		})
		if err != nil {
			return authentication.NewListUserGroupsDefault(http.StatusInternalServerError).
				WithPayload(responseErrorFrom(err))
		}

		response := make([]*models.Group, len(groups))
		for i, g := range groups {
			response[i] = &models.Group{
				CreationDate: g.CreatedAt.Unix(),
				ID:           g.DisplayName,
			}
		}

		return authentication.NewListUserGroupsOK().
			WithPayload(&authentication.ListUserGroupsOKBody{
				Pagination: createPaginator(paginator.NextPageToken, len(response)),
				Results:    response,
			})
	})
}

func (c *Controller) ListUserPoliciesHandler() authentication.ListUserPoliciesHandler {
	return authentication.ListUserPoliciesHandlerFunc(func(params authentication.ListUserPoliciesParams, user *models.User) middleware.Responder {
		deps, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.ReadUserAction,
				Resource: permissions.UserArn(params.UserID),
			},
		})
		if err != nil {
			return authentication.NewListUserPoliciesUnauthorized().
				WithPayload(responseErrorFrom(err))
		}

		deps.LogAction("list_user_policies")
		var policies []*authmodel.Policy
		var paginator *authmodel.Paginator
		if swag.BoolValue(params.Effective) {
			policies, paginator, err = deps.Auth.ListEffectivePolicies(params.UserID, &authmodel.PaginationParams{
				After:  swag.StringValue(params.After),
				Amount: pageAmount(params.Amount),
			})
		} else {
			policies, paginator, err = deps.Auth.ListUserPolicies(params.UserID, &authmodel.PaginationParams{
				After:  swag.StringValue(params.After),
				Amount: pageAmount(params.Amount),
			})
		}

		if err != nil {
			return authentication.NewListUserPoliciesDefault(http.StatusInternalServerError).
				WithPayload(responseErrorFrom(err))
		}

		response := make([]*models.Policy, len(policies))
		for i, p := range policies {
			response[i] = serializePolicy(p)
		}

		return authentication.NewListUserPoliciesOK().
			WithPayload(&authentication.ListUserPoliciesOKBody{
				Pagination: createPaginator(paginator.NextPageToken, len(response)),
				Results:    response,
			})
	})
}

func (c *Controller) AttachPolicyToUserHandler() authentication.AttachPolicyToUserHandler {
	return authentication.AttachPolicyToUserHandlerFunc(func(params authentication.AttachPolicyToUserParams, user *models.User) middleware.Responder {
		deps, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.AttachPolicyAction,
				Resource: permissions.UserArn(params.UserID),
			},
		})
		if err != nil {
			return authentication.NewAttachPolicyToUserUnauthorized().
				WithPayload(responseErrorFrom(err))
		}

		deps.LogAction("attach_policy_to_user")
		err = deps.Auth.AttachPolicyToUser(params.PolicyID, params.UserID)
		if err != nil {
			return authentication.NewAttachPolicyToUserDefault(http.StatusInternalServerError).
				WithPayload(responseErrorFrom(err))
		}

		return authentication.NewAttachPolicyToUserCreated()
	})
}

func (c *Controller) DetachPolicyFromUserHandler() authentication.DetachPolicyFromUserHandler {
	return authentication.DetachPolicyFromUserHandlerFunc(func(params authentication.DetachPolicyFromUserParams, user *models.User) middleware.Responder {
		deps, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.DetachPolicyAction,
				Resource: permissions.UserArn(params.UserID),
			},
		})
		if err != nil {
			return authentication.NewDetachPolicyFromUserUnauthorized().
				WithPayload(responseErrorFrom(err))
		}

		deps.LogAction("detach_policy_from_user")
		err = deps.Auth.DetachPolicyFromUser(params.PolicyID, params.UserID)
		if err != nil {
			return authentication.NewDetachPolicyFromUserDefault(http.StatusInternalServerError).
				WithPayload(responseErrorFrom(err))
		}

		return authentication.NewDetachPolicyFromUserNoContent()
	})
}

func (c *Controller) ListGroupPoliciesHandler() authentication.ListGroupPoliciesHandler {
	return authentication.ListGroupPoliciesHandlerFunc(func(params authentication.ListGroupPoliciesParams, user *models.User) middleware.Responder {
		deps, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.ReadGroupAction,
				Resource: permissions.GroupArn(params.GroupID),
			},
		})
		if err != nil {
			return authentication.NewListGroupPoliciesUnauthorized().
				WithPayload(responseErrorFrom(err))
		}

		deps.LogAction("list_user_policies")
		policies, paginator, err := deps.Auth.ListGroupPolicies(params.GroupID, &authmodel.PaginationParams{
			After:  swag.StringValue(params.After),
			Amount: pageAmount(params.Amount),
		})
		if err != nil {
			return authentication.NewListGroupPoliciesDefault(http.StatusInternalServerError).
				WithPayload(responseErrorFrom(err))
		}

		response := make([]*models.Policy, len(policies))
		for i, p := range policies {
			response[i] = serializePolicy(p)
		}

		return authentication.NewListGroupPoliciesOK().
			WithPayload(&authentication.ListGroupPoliciesOKBody{
				Pagination: createPaginator(paginator.NextPageToken, len(response)),
				Results:    response,
			})
	})
}

func (c *Controller) AttachPolicyToGroupHandler() authentication.AttachPolicyToGroupHandler {
	return authentication.AttachPolicyToGroupHandlerFunc(func(params authentication.AttachPolicyToGroupParams, user *models.User) middleware.Responder {
		deps, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.AttachPolicyAction,
				Resource: permissions.GroupArn(params.GroupID),
			},
		})
		if err != nil {
			return authentication.NewAttachPolicyToGroupUnauthorized().
				WithPayload(responseErrorFrom(err))
		}

		deps.LogAction("attach_policy_to_group")
		err = deps.Auth.AttachPolicyToGroup(params.PolicyID, params.GroupID)
		if err != nil {
			return authentication.NewAttachPolicyToGroupDefault(http.StatusInternalServerError).
				WithPayload(responseErrorFrom(err))
		}

		return authentication.NewAttachPolicyToGroupCreated()
	})
}

func (c *Controller) DetachPolicyFromGroupHandler() authentication.DetachPolicyFromGroupHandler {
	return authentication.DetachPolicyFromGroupHandlerFunc(func(params authentication.DetachPolicyFromGroupParams, user *models.User) middleware.Responder {
		deps, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.DetachPolicyAction,
				Resource: permissions.GroupArn(params.GroupID),
			},
		})
		if err != nil {
			return authentication.NewDetachPolicyFromGroupUnauthorized().
				WithPayload(responseErrorFrom(err))
		}

		deps.LogAction("detach_policy_from_group")
		err = deps.Auth.DetachPolicyFromGroup(params.PolicyID, params.GroupID)
		if err != nil {
			return authentication.NewDetachPolicyFromGroupDefault(http.StatusInternalServerError).
				WithPayload(responseErrorFrom(err))
		}

		return authentication.NewDetachPolicyFromGroupNoContent()
	})
}
