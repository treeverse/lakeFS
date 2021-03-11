package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/go-openapi/runtime"
	"github.com/go-openapi/runtime/middleware"
	"github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"
	"github.com/treeverse/lakefs/pkg/actions"
	"github.com/treeverse/lakefs/pkg/api/gen/models"
	"github.com/treeverse/lakefs/pkg/api/gen/restapi/operations"
	actionsop "github.com/treeverse/lakefs/pkg/api/gen/restapi/operations/actions"
	authop "github.com/treeverse/lakefs/pkg/api/gen/restapi/operations/auth"
	"github.com/treeverse/lakefs/pkg/api/gen/restapi/operations/branches"
	"github.com/treeverse/lakefs/pkg/api/gen/restapi/operations/commits"
	configop "github.com/treeverse/lakefs/pkg/api/gen/restapi/operations/config"
	hcop "github.com/treeverse/lakefs/pkg/api/gen/restapi/operations/health_check"
	"github.com/treeverse/lakefs/pkg/api/gen/restapi/operations/metadata"
	"github.com/treeverse/lakefs/pkg/api/gen/restapi/operations/objects"
	"github.com/treeverse/lakefs/pkg/api/gen/restapi/operations/refs"
	"github.com/treeverse/lakefs/pkg/api/gen/restapi/operations/repositories"
	setupop "github.com/treeverse/lakefs/pkg/api/gen/restapi/operations/setup"
	"github.com/treeverse/lakefs/pkg/api/gen/restapi/operations/tags"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/cloud"
	"github.com/treeverse/lakefs/pkg/db"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/httputil"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/permissions"
	"github.com/treeverse/lakefs/pkg/stats"
	"github.com/treeverse/lakefs/pkg/upload"
)

type contextKey string

const (
	// Maximum amount of results returned for paginated queries to the API
	MaxResultsPerPage                = 1000
	DefaultResultsPerPage            = 100
	lakeFSPrefix                     = "symlinks"
	UserContextKey        contextKey = "user"
)

type actionsHandler interface {
	GetRunResult(ctx context.Context, repositoryID string, runID string) (*actions.RunResult, error)
	GetTaskResult(ctx context.Context, repositoryID string, runID string, hookRunID string) (*actions.TaskResult, error)
	ListRunResults(ctx context.Context, repositoryID string, branchID, commitID string, after string) (actions.RunResultIterator, error)
	ListRunTaskResults(ctx context.Context, repositoryID string, runID string, after string) (actions.TaskResultIterator, error)
}

type Controller struct {
	Catalog               catalog.Interface
	Auth                  auth.Service
	BlockAdapter          block.Adapter
	MetadataManager       auth.MetadataManager
	Migrator              db.Migrator
	Collector             stats.Collector
	CloudMetadataProvider cloud.MetadataProvider
	Actions               actionsHandler
	Logger                logging.Logger
}

func NewController(
	catalog catalog.Interface,
	authService auth.Service,
	blockAdapter block.Adapter,
	metadataManager auth.MetadataManager,
	migrator db.Migrator,
	collector stats.Collector,
	cloudMetadataProvider cloud.MetadataProvider,
	actions actionsHandler,
	logger logging.Logger,
) *Controller {
	return &Controller{
		Catalog:               catalog,
		Auth:                  authService,
		BlockAdapter:          blockAdapter,
		MetadataManager:       metadataManager,
		Migrator:              migrator,
		Collector:             collector,
		CloudMetadataProvider: cloudMetadataProvider,
		Actions:               actions,
		Logger:                logger,
	}
}

func (c *Controller) LogAction(ctx context.Context, action string) {
	c.Logger.WithContext(ctx).
		WithField("action", action).
		WithField("message_type", "action").
		Debug("performing API action")
	c.Collector.CollectEvent("api_server", action)
}

// Configure attaches our API operations to a generated swagger API stub
// Adding new handlers requires also adding them here so that the generated server will use them
func (c *Controller) Configure(api *operations.LakefsAPI) {
	// Register operations here
	api.HealthCheckHealthCheckHandler = c.GetHealthCheckHandler()
	api.SetupSetupLakeFSHandler = c.SetupLakeFSHandler()

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
	api.BranchesResetBranchHandler = c.ResetBranchHandler()
	api.BranchesRevertHandler = c.RevertHandler()

	api.TagsListTagsHandler = c.ListTagsHandler()
	api.TagsGetTagHandler = c.GetTagHandler()
	api.TagsCreateTagHandler = c.CreateTagHandler()
	api.TagsDeleteTagHandler = c.DeleteTagHandler()

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
	api.ObjectsStageObjectHandler = c.ObjectsStageObjectHandler()
	api.ObjectsDeleteObjectHandler = c.ObjectsDeleteObjectHandler()

	api.MetadataCreateSymlinkHandler = c.MetadataCreateSymlinkHandler()
	api.MetadataGetRangeHandler = c.MetadataGetRangeHandler()
	api.MetadataGetMetaRangeHandler = c.MetadataGetMetarangeHandler()

	api.ConfigGetConfigHandler = c.ConfigGetConfigHandler()

	api.RefsDumpHandler = c.RefsDumpHandler()
	api.RefsRestoreHandler = c.RefsRestoreHandler()

	api.ActionsGetRunHandler = c.ActionsGetRunHandler()
	api.ActionsGetRunHookOutputHandler = c.ActionsGetRunHookOutputHandler()
	api.ActionsListRunHooksHandler = c.ActionsListRunHooksHandler()
	api.ActionsListRunsHandler = c.ActionsListRunsHandler()
}

func (c *Controller) setupRequest(user *models.User, r *http.Request, permissions []permissions.Permission) (context.Context, error) {
	// add user to context
	ctx := logging.AddFields(r.Context(), logging.Fields{"user": user.ID})
	ctx = context.WithValue(ctx, UserContextKey, user)
	return ctx, authorize(ctx, c.Auth, user, permissions)
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
	if inti > MaxResultsPerPage {
		return MaxResultsPerPage
	}
	if inti <= 0 {
		return DefaultResultsPerPage
	}
	return inti
}

func (c *Controller) GetHealthCheckHandler() hcop.HealthCheckHandler {
	return hcop.HealthCheckHandlerFunc(func(params hcop.HealthCheckParams) middleware.Responder {
		return hcop.NewHealthCheckNoContent()
	})
}

func (c *Controller) SetupLakeFSHandler() setupop.SetupLakeFSHandler {
	return setupop.SetupLakeFSHandlerFunc(func(setupReq setupop.SetupLakeFSParams) middleware.Responder {
		if len(*setupReq.User.Username) == 0 {
			return setupop.NewSetupLakeFSBadRequest().
				WithPayload(&models.Error{
					Message: "empty display name",
				})
		}

		// check if previous setup completed
		if ts, _ := c.MetadataManager.SetupTimestamp(setupReq.HTTPRequest.Context()); !ts.IsZero() {
			return setupop.NewSetupLakeFSConflict().
				WithPayload(&models.Error{
					Message: "lakeFS already initialized",
				})
		}

		// migrate the database if needed
		ctx := setupReq.HTTPRequest.Context()
		err := c.Migrator.Migrate(ctx)
		if err != nil {
			return setupop.NewSetupLakeFSDefault(http.StatusInternalServerError).
				WithPayload(&models.Error{
					Message: err.Error(),
				})
		}

		username := swag.StringValue(setupReq.User.Username)
		var cred *model.Credential
		if setupReq.User.Key == nil {
			cred, err = auth.CreateInitialAdminUser(ctx, c.Auth, c.MetadataManager, username)
		} else {
			cred, err = auth.CreateInitialAdminUserWithKeys(ctx, c.Auth, c.MetadataManager, username, setupReq.User.Key.AccessKeyID, setupReq.User.Key.SecretAccessKey)
		}
		if err != nil {
			return setupop.NewSetupLakeFSDefault(http.StatusInternalServerError).
				WithPayload(&models.Error{Message: err.Error()})
		}

		meta := stats.NewMetadata(ctx, c.Logger, c.BlockAdapter.BlockstoreType(), c.MetadataManager, c.CloudMetadataProvider)
		c.Collector.SetInstallationID(meta.InstallationID)
		c.Collector.CollectMetadata(meta)
		c.Collector.CollectEvent("global", "init")

		return setupop.NewSetupLakeFSOK().WithPayload(&models.CredentialsWithSecret{
			AccessKeyID:     cred.AccessKeyID,
			AccessSecretKey: cred.AccessSecretKey,
			CreationDate:    cred.IssuedDate.Unix(),
		})
	})
}

func (c *Controller) GetCurrentUserHandler() authop.GetCurrentUserHandler {
	return authop.GetCurrentUserHandlerFunc(func(params authop.GetCurrentUserParams, user *models.User) middleware.Responder {
		return authop.NewGetCurrentUserOK().WithPayload(&authop.GetCurrentUserOKBody{
			User: user,
		})
	})
}

func (c *Controller) ListRepositoriesHandler() repositories.ListRepositoriesHandler {
	return repositories.ListRepositoriesHandlerFunc(func(params repositories.ListRepositoriesParams, user *models.User) middleware.Responder {
		ctx, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.ListRepositoriesAction,
				Resource: permissions.All,
			},
		})

		if err != nil {
			return repositories.NewListRepositoriesUnauthorized().WithPayload(responseErrorFrom(err))
		}
		c.LogAction(ctx, "list_repos")

		after, amount := getPaginationParams(params.After, params.Amount)

		repos, hasMore, err := c.Catalog.ListRepositories(ctx, amount, after)
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
				ID:               repo.Name,
			}
			lastID = repo.Name
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
	amount := int(swag.Int64Value(swagAmount))
	if swagAmount == nil || amount < 0 || amount > MaxResultsPerPage {
		amount = MaxResultsPerPage
	}

	// paginate after
	after := swag.StringValue(swagAfter)
	return after, amount
}

func (c *Controller) GetRepoHandler() repositories.GetRepositoryHandler {
	return repositories.GetRepositoryHandlerFunc(func(params repositories.GetRepositoryParams, user *models.User) middleware.Responder {
		ctx, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.ReadRepositoryAction,
				Resource: permissions.RepoArn(params.Repository),
			},
		})
		if err != nil {
			return repositories.NewGetRepositoryUnauthorized().WithPayload(responseErrorFrom(err))
		}
		c.LogAction(ctx, "get_repo")
		repo, err := c.Catalog.GetRepository(ctx, params.Repository)
		if errors.Is(err, catalog.ErrNotFound) {
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
				ID:               repo.Name,
			})
	})
}

func (c *Controller) GetCommitHandler() commits.GetCommitHandler {
	return commits.GetCommitHandlerFunc(func(params commits.GetCommitParams, user *models.User) middleware.Responder {
		ctx, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.ReadCommitAction,
				Resource: permissions.RepoArn(params.Repository),
			},
		})
		if err != nil {
			return commits.NewGetCommitUnauthorized().WithPayload(responseErrorFrom(err))
		}
		c.LogAction(ctx, "get_commit")
		commit, err := c.Catalog.GetCommit(ctx, params.Repository, params.CommitID)
		if errors.Is(err, catalog.ErrNotFound) {
			return commits.NewGetCommitNotFound().WithPayload(responseError("commit not found"))
		}
		if err != nil {
			return commits.NewGetCommitDefault(http.StatusInternalServerError).WithPayload(responseErrorFrom(err))
		}
		return commits.NewGetCommitOK().WithPayload(&models.Commit{
			Committer:    commit.Committer,
			CreationDate: commit.CreationDate.Unix(),
			ID:           params.CommitID,
			Message:      commit.Message,
			Metadata:     commit.Metadata,
			Parents:      commit.Parents,
			MetaRangeID:  commit.MetaRangeID,
		})
	})
}

func (c *Controller) CommitHandler() commits.CommitHandler {
	return commits.CommitHandlerFunc(func(params commits.CommitParams, user *models.User) middleware.Responder {
		ctx, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.CreateCommitAction,
				Resource: permissions.BranchArn(params.Repository, params.Branch),
			},
		})
		if err != nil {
			return commits.NewCommitUnauthorized().WithPayload(responseErrorFrom(err))
		}
		c.LogAction(ctx, "create_commit")
		userModel, err := c.Auth.GetUser(ctx, user.ID)
		if err != nil {
			return commits.NewCommitUnauthorized().WithPayload(responseErrorFrom(err))
		}
		committer := userModel.Username
		commitMessage := swag.StringValue(params.Commit.Message)
		commit, err := c.Catalog.Commit(ctx, params.Repository,
			params.Branch, commitMessage, committer, params.Commit.Metadata)
		var hookAbortErr *graveler.HookAbortError
		if errors.As(err, &hookAbortErr) {
			c.Logger.
				WithError(err).
				WithField("run_id", hookAbortErr.RunID).
				Warn("aborted by hooks")
			return commits.NewCommitPreconditionFailed().WithPayload(responseErrorFrom(err))
		}
		if err != nil {
			return commits.NewCommitDefault(http.StatusInternalServerError).WithPayload(responseErrorFrom(err))
		}
		return commits.NewCommitCreated().WithPayload(&models.Commit{
			Committer:    commit.Committer,
			CreationDate: commit.CreationDate.Unix(),
			ID:           commit.Reference,
			Message:      commit.Message,
			Metadata:     commit.Metadata,
			Parents:      commit.Parents,
		})
	})
}

func (c *Controller) CommitsGetBranchCommitLogHandler() commits.GetBranchCommitLogHandler {
	return commits.GetBranchCommitLogHandlerFunc(func(params commits.GetBranchCommitLogParams, user *models.User) middleware.Responder {
		ctx, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.ReadBranchAction,
				Resource: permissions.BranchArn(params.Repository, params.Branch),
			},
		})
		if err != nil {
			return commits.NewGetBranchCommitLogUnauthorized().WithPayload(responseErrorFrom(err))
		}
		c.LogAction(ctx, "get_branch_commit_log")

		after, amount := getPaginationParams(params.After, params.Amount)
		// get commit log
		commitLog, hasMore, err := c.Catalog.ListCommits(ctx, params.Repository, params.Branch, after, amount)
		switch {
		case errors.Is(err, catalog.ErrBranchNotFound) || errors.Is(err, graveler.ErrBranchNotFound):
			return commits.NewGetBranchCommitLogNotFound().WithPayload(responseError("branch '%s' not found.", params.Branch))
		case errors.Is(err, catalog.ErrRepositoryNotFound) || errors.Is(err, graveler.ErrRepositoryNotFound):
			return commits.NewGetBranchCommitLogNotFound().WithPayload(responseError("repository '%s' not found.", params.Repository))
		case err != nil:
			return commits.NewGetBranchCommitLogDefault(http.StatusInternalServerError).WithPayload(responseErrorFrom(err))
		}

		serializedCommits := make([]*models.Commit, len(commitLog))
		lastID := ""
		for i, commit := range commitLog {
			serializedCommits[i] = &models.Commit{
				Committer:    commit.Committer,
				CreationDate: commit.CreationDate.Unix(),
				ID:           commit.Reference,
				Message:      commit.Message,
				Metadata:     commit.Metadata,
				MetaRangeID:  commit.MetaRangeID,
				Parents:      commit.Parents,
			}
			lastID = commit.Reference
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
			returnValue.Payload.Pagination.NextOffset = lastID
		}
		return returnValue
	})
}

func ensureStorageNamespaceRW(ctx context.Context, adapter block.Adapter, storageNamespace string) error {
	const (
		dummyKey  = "dummy"
		dummyData = "this is dummy data - created by lakeFS in order to check accessibility "
	)

	err := adapter.Put(ctx, block.ObjectPointer{StorageNamespace: storageNamespace, Identifier: dummyKey}, int64(len(dummyData)), bytes.NewReader([]byte(dummyData)), block.PutOpts{})
	if err != nil {
		return err
	}

	_, err = adapter.Get(ctx, block.ObjectPointer{StorageNamespace: storageNamespace, Identifier: dummyKey}, int64(len(dummyData)))
	if err != nil {
		return err
	}

	return nil
}

func (c *Controller) CreateRepositoryHandler() repositories.CreateRepositoryHandler {
	return repositories.CreateRepositoryHandlerFunc(func(params repositories.CreateRepositoryParams, user *models.User) middleware.Responder {
		ctx, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.CreateRepositoryAction,
				Resource: permissions.RepoArn(swag.StringValue(params.Repository.Name)),
			},
		})
		if err != nil {
			return repositories.NewCreateRepositoryUnauthorized().WithPayload(responseErrorFrom(err))
		}
		c.LogAction(ctx, "create_repo")

		if swag.BoolValue(params.Bare) {
			// create a bare repository. This is useful in conjunction with refs-restore to create a copy
			// of another repository by e.g. copying the _lakefs/ directory and restoring its refs
			repo, err := c.Catalog.CreateBareRepository(ctx,
				swag.StringValue(params.Repository.Name),
				swag.StringValue(params.Repository.StorageNamespace),
				params.Repository.DefaultBranch)
			if err != nil {
				c.Logger.
					WithError(err).
					WithField("storage_namespace", swag.StringValue(params.Repository.StorageNamespace)).
					Warn("Could not access storage namespace")
				return repositories.NewCreateRepositoryBadRequest().
					WithPayload(responseError("error creating repository: could not access storage namespace"))
			}
			return repositories.NewCreateRepositoryCreated().WithPayload(&models.Repository{
				StorageNamespace: repo.StorageNamespace,
				CreationDate:     repo.CreationDate.Unix(),
				DefaultBranch:    repo.DefaultBranch,
				ID:               repo.Name,
			})
		}

		err = ensureStorageNamespaceRW(ctx, c.BlockAdapter, swag.StringValue(params.Repository.StorageNamespace))
		if err != nil {
			c.Logger.
				WithError(err).
				WithField("storage_namespace", swag.StringValue(params.Repository.StorageNamespace)).
				Warn("Could not access storage namespace")
			return repositories.NewCreateRepositoryBadRequest().
				WithPayload(responseError("error creating repository: could not access storage namespace"))
		}
		repo, err := c.Catalog.CreateRepository(ctx,
			swag.StringValue(params.Repository.Name),
			swag.StringValue(params.Repository.StorageNamespace),
			params.Repository.DefaultBranch)
		if err != nil {
			return repositories.NewGetRepositoryDefault(http.StatusInternalServerError).
				WithPayload(responseError(fmt.Sprintf("error creating repository: %s", err)))
		}

		return repositories.NewCreateRepositoryCreated().WithPayload(&models.Repository{
			StorageNamespace: repo.StorageNamespace,
			CreationDate:     repo.CreationDate.Unix(),
			DefaultBranch:    repo.DefaultBranch,
			ID:               repo.Name,
		})
	})
}

func (c *Controller) DeleteRepositoryHandler() repositories.DeleteRepositoryHandler {
	return repositories.DeleteRepositoryHandlerFunc(func(params repositories.DeleteRepositoryParams, user *models.User) middleware.Responder {
		ctx, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.DeleteRepositoryAction,
				Resource: permissions.RepoArn(params.Repository),
			},
		})
		if err != nil {
			return repositories.NewDeleteRepositoryUnauthorized().WithPayload(responseErrorFrom(err))
		}
		c.LogAction(ctx, "delete_repo")
		err = c.Catalog.DeleteRepository(ctx, params.Repository)
		if errors.Is(err, catalog.ErrNotFound) {
			return repositories.NewDeleteRepositoryNotFound().WithPayload(responseError("repository not found"))
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
		ctx, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.ListBranchesAction,
				Resource: permissions.RepoArn(params.Repository),
			},
		})
		if err != nil {
			return branches.NewListBranchesUnauthorized().WithPayload(responseErrorFrom(err))
		}
		c.LogAction(ctx, "list_branches")

		after, amount := getPaginationParams(params.After, params.Amount)

		res, hasMore, err := c.Catalog.ListBranches(ctx, params.Repository, "", amount, after)
		if err != nil {
			return branches.NewListBranchesDefault(http.StatusInternalServerError).
				WithPayload(responseError("could not list branches: %s", err))
		}

		branchList := make([]*models.Ref, len(res))
		var lastID string
		for i, branch := range res {
			branchList[i] = &models.Ref{
				CommitID: swag.String(branch.Reference),
				ID:       swag.String(branch.Name),
			}
			lastID = branch.Name
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
			returnValue.Payload.Pagination.NextOffset = lastID
		}

		return returnValue
	})
}

func (c *Controller) GetBranchHandler() branches.GetBranchHandler {
	return branches.GetBranchHandlerFunc(func(params branches.GetBranchParams, user *models.User) middleware.Responder {
		ctx, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.ReadBranchAction,
				Resource: permissions.BranchArn(params.Repository, params.Branch),
			},
		})
		if err != nil {
			return branches.NewGetBranchUnauthorized().WithPayload(responseErrorFrom(err))
		}
		c.LogAction(ctx, "get_branch")
		reference, err := c.Catalog.GetBranchReference(ctx, params.Repository, params.Branch)

		switch {
		case errors.Is(err, catalog.ErrBranchNotFound) || errors.Is(err, graveler.ErrBranchNotFound):
			return branches.NewGetBranchNotFound().WithPayload(responseError("branch '%s' not found.", params.Branch))
		case errors.Is(err, catalog.ErrRepositoryNotFound) || errors.Is(err, graveler.ErrRepositoryNotFound):
			return branches.NewGetBranchNotFound().WithPayload(responseError("repository '%s' not found.", params.Repository))
		case err != nil:
			return branches.NewGetBranchDefault(http.StatusInternalServerError).WithPayload(responseErrorFrom(err))
		}

		return branches.NewGetBranchOK().WithPayload(&models.Ref{
			CommitID: swag.String(reference),
			ID:       swag.String(params.Branch),
		})
	})
}

func (c *Controller) CreateBranchHandler() branches.CreateBranchHandler {
	return branches.CreateBranchHandlerFunc(func(params branches.CreateBranchParams, user *models.User) middleware.Responder {
		repository := params.Repository
		branch := swag.StringValue(params.Branch.Name)
		ctx, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.CreateBranchAction,
				Resource: permissions.BranchArn(repository, branch),
			},
		})
		if err != nil {
			return branches.NewCreateBranchUnauthorized().WithPayload(responseErrorFrom(err))
		}
		c.LogAction(ctx, "create_branch")
		sourceRef := swag.StringValue(params.Branch.Source)
		commitLog, err := c.Catalog.CreateBranch(ctx, repository, branch, sourceRef)
		if err != nil {
			return branches.NewCreateBranchDefault(http.StatusInternalServerError).WithPayload(responseErrorFrom(err))
		}
		return branches.NewCreateBranchCreated().WithPayload(commitLog.Reference)
	})
}

func (c *Controller) DeleteBranchHandler() branches.DeleteBranchHandler {
	return branches.DeleteBranchHandlerFunc(func(params branches.DeleteBranchParams, user *models.User) middleware.Responder {
		ctx, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.DeleteBranchAction,
				Resource: permissions.BranchArn(params.Repository, params.Branch),
			},
		})
		if err != nil {
			return branches.NewDeleteBranchUnauthorized().WithPayload(responseErrorFrom(err))
		}
		c.LogAction(ctx, "delete_branch")
		err = c.Catalog.DeleteBranch(ctx, params.Repository, params.Branch)
		switch {
		case errors.Is(err, catalog.ErrBranchNotFound) || errors.Is(err, graveler.ErrBranchNotFound):
			return branches.NewDeleteBranchNotFound().WithPayload(responseError("branch '%s' not found.", params.Branch))
		case errors.Is(err, catalog.ErrRepositoryNotFound) || errors.Is(err, graveler.ErrRepositoryNotFound):
			return branches.NewDeleteBranchNotFound().WithPayload(responseError("repository '%s' not found.", params.Repository))
		case err != nil:
			return branches.NewDeleteBranchDefault(http.StatusInternalServerError).WithPayload(responseErrorFrom(err))
		}

		return branches.NewDeleteBranchNoContent()
	})
}

func (c *Controller) ListTagsHandler() tags.ListTagsHandler {
	return tags.ListTagsHandlerFunc(func(params tags.ListTagsParams, user *models.User) middleware.Responder {
		ctx, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.ListTagsAction,
				Resource: permissions.RepoArn(params.Repository),
			},
		})
		if err != nil {
			return tags.NewListTagsUnauthorized().WithPayload(responseErrorFrom(err))
		}
		c.LogAction(ctx, "list_tags")
		after, amount := getPaginationParams(params.After, params.Amount)

		res, hasMore, err := c.Catalog.ListTags(ctx, params.Repository, amount, after)
		if err != nil {
			return tags.NewListTagsDefault(http.StatusInternalServerError).
				WithPayload(responseError("could not list tags: %s", err))
		}

		tagList := make([]*models.Ref, len(res))
		var lastID string
		for i, tag := range res {
			tagList[i] = &models.Ref{
				CommitID: swag.String(tag.CommitID),
				ID:       swag.String(tag.ID),
			}
			lastID = tag.ID
		}
		returnValue := tags.NewListTagsOK().WithPayload(&tags.ListTagsOKBody{
			Pagination: &models.Pagination{
				HasMore:    swag.Bool(hasMore),
				Results:    swag.Int64(int64(len(tagList))),
				MaxPerPage: swag.Int64(MaxResultsPerPage),
			},
			Results: tagList,
		})
		if hasMore {
			returnValue.Payload.Pagination.NextOffset = lastID
		}
		return returnValue
	})
}

func (c *Controller) GetTagHandler() tags.GetTagHandler {
	return tags.GetTagHandlerFunc(func(params tags.GetTagParams, user *models.User) middleware.Responder {
		ctx, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.ReadTagAction,
				Resource: permissions.TagArn(params.Repository, params.Tag),
			},
		})
		if err != nil {
			return tags.NewGetTagUnauthorized().WithPayload(responseErrorFrom(err))
		}
		c.LogAction(ctx, "get_tag")
		reference, err := c.Catalog.GetTag(ctx, params.Repository, params.Tag)

		switch {
		case errors.Is(err, graveler.ErrTagNotFound):
			return tags.NewGetTagNotFound().WithPayload(responseError("tag '%s' not found.", params.Tag))
		case errors.Is(err, graveler.ErrRepositoryNotFound):
			return tags.NewGetTagNotFound().WithPayload(responseError("repository '%s' not found.", params.Repository))
		case err != nil:
			return tags.NewGetTagDefault(http.StatusInternalServerError).WithPayload(responseErrorFrom(err))
		}

		return tags.NewGetTagOK().WithPayload(&models.Ref{
			CommitID: swag.String(reference),
			ID:       swag.String(params.Tag),
		})
	})
}

func (c *Controller) CreateTagHandler() tags.CreateTagHandler {
	return tags.CreateTagHandlerFunc(func(params tags.CreateTagParams, user *models.User) middleware.Responder {
		repository := params.Repository
		tagID := swag.StringValue(params.Tag.ID)
		ctx, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.CreateTagAction,
				Resource: permissions.TagArn(repository, tagID),
			},
		})
		if err != nil {
			return tags.NewCreateTagUnauthorized().WithPayload(responseErrorFrom(err))
		}
		c.LogAction(ctx, "create_tag")
		tagRef := swag.StringValue(params.Tag.Ref)
		commitID, err := c.Catalog.CreateTag(ctx, repository, tagID, tagRef)
		if err != nil {
			return tags.NewCreateTagDefault(http.StatusInternalServerError).WithPayload(responseErrorFrom(err))
		}
		return tags.NewCreateTagCreated().WithPayload(&models.Ref{
			CommitID: swag.String(commitID),
			ID:       swag.String(tagID),
		})
	})
}

func (c *Controller) DeleteTagHandler() tags.DeleteTagHandler {
	return tags.DeleteTagHandlerFunc(func(params tags.DeleteTagParams, user *models.User) middleware.Responder {
		ctx, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.DeleteTagAction,
				Resource: permissions.TagArn(params.Repository, params.Tag),
			},
		})
		if err != nil {
			return tags.NewDeleteTagUnauthorized().WithPayload(responseErrorFrom(err))
		}
		c.LogAction(ctx, "delete_tag")
		err = c.Catalog.DeleteTag(ctx, params.Repository, params.Tag)
		switch {
		case errors.Is(err, graveler.ErrTagNotFound):
			return tags.NewDeleteTagNotFound().WithPayload(responseError("tag '%s' not found.", params.Tag))
		case errors.Is(err, graveler.ErrRepositoryNotFound):
			return tags.NewDeleteTagNotFound().WithPayload(responseError("repository '%s' not found.", params.Repository))
		case err != nil:
			return tags.NewDeleteTagDefault(http.StatusInternalServerError).WithPayload(responseErrorFrom(err))
		}
		return tags.NewDeleteTagNoContent()
	})
}

func (c *Controller) MergeMergeIntoBranchHandler() refs.MergeIntoBranchHandler {
	return refs.MergeIntoBranchHandlerFunc(func(params refs.MergeIntoBranchParams, user *models.User) middleware.Responder {
		ctx, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.CreateCommitAction,
				Resource: permissions.BranchArn(params.Repository, params.DestinationBranch),
			},
		})
		if err != nil {
			return refs.NewMergeIntoBranchUnauthorized().WithPayload(responseErrorFrom(err))
		}
		c.LogAction(ctx, "merge_branches")
		userModel, err := c.Auth.GetUser(ctx, user.ID)
		if err != nil {
			return refs.NewMergeIntoBranchUnauthorized().WithPayload(responseErrorFrom(err))
		}
		var message string
		var meta map[string]string
		if params.Merge != nil {
			message = params.Merge.Message
			meta = params.Merge.Metadata
		}
		res, err := c.Catalog.Merge(ctx,
			params.Repository, params.DestinationBranch, params.SourceRef,
			userModel.Username,
			message,
			meta)

		var hookAbortErr *graveler.HookAbortError
		if errors.As(err, &hookAbortErr) {
			c.Logger.
				WithError(err).
				WithField("run_id", hookAbortErr.RunID).
				Warn("aborted by hooks")
			return refs.NewMergeIntoBranchPreconditionFailed().WithPayload(responseErrorFrom(err))
		}
		switch {
		case err == nil:
			payload := newMergeResultFromCatalog(res)
			return refs.NewMergeIntoBranchOK().WithPayload(payload)
		case errors.Is(err, catalog.ErrBranchNotFound) || errors.Is(err, graveler.ErrBranchNotFound):
			return refs.NewMergeIntoBranchBadRequest().WithPayload(responseError("a branch does not exist"))
		case errors.Is(err, catalog.ErrConflictFound) || errors.Is(err, graveler.ErrConflictFound):
			payload := newMergeResultFromCatalog(res)
			return refs.NewMergeIntoBranchConflict().WithPayload(payload)
		case errors.Is(err, catalog.ErrNoDifferenceWasFound) || errors.Is(err, graveler.ErrNoChanges):
			return refs.NewMergeIntoBranchBadRequest().WithPayload(responseError("no difference was found"))
		case errors.Is(err, graveler.ErrLockNotAcquired):
			return refs.NewMergeIntoBranchDefault(http.StatusInternalServerError).WithPayload(responseError("branch is currently locked, try again later"))
		case errors.Is(err, graveler.ErrDirtyBranch):
			return refs.NewMergeIntoBranchBadRequest().WithPayload(responseError("can't merge into a dirty branch"))
		default:
			return refs.NewMergeIntoBranchDefault(http.StatusInternalServerError).WithPayload(responseErrorFrom(err))
		}
	})
}

func newMergeResultFromCatalog(res *catalog.MergeResult) *models.MergeResult {
	if res == nil {
		return &models.MergeResult{}
	}
	var summary models.MergeResultSummary
	for k, v := range res.Summary {
		val := int64(v)
		switch k {
		case catalog.DifferenceTypeAdded:
			summary.Added = val
		case catalog.DifferenceTypeChanged:
			summary.Changed = val
		case catalog.DifferenceTypeRemoved:
			summary.Removed = val
		case catalog.DifferenceTypeConflict:
			summary.Conflict = val
		}
	}
	return &models.MergeResult{
		Reference: res.Reference,
		Summary:   &summary,
	}
}

func (c *Controller) BranchesDiffBranchHandler() branches.DiffBranchHandler {
	return branches.DiffBranchHandlerFunc(func(params branches.DiffBranchParams, user *models.User) middleware.Responder {
		ctx, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.ListObjectsAction,
				Resource: permissions.RepoArn(params.Repository),
			},
		})
		if err != nil {
			return branches.NewDiffBranchUnauthorized().WithPayload(responseErrorFrom(err))
		}
		c.LogAction(ctx, "diff_workspace")
		limit := int(swag.Int64Value(params.Amount))
		after := swag.StringValue(params.After)
		diff, hasMore, err := c.Catalog.DiffUncommitted(ctx, params.Repository, params.Branch, limit, after)
		if errors.Is(err, catalog.ErrNotFound) {
			return branches.NewDiffBranchNotFound().
				WithPayload(responseError("could not diff branch: %s", err))
		}
		if err != nil {
			return branches.NewDiffBranchDefault(http.StatusInternalServerError).
				WithPayload(responseError("could not diff branch: %s", err))
		}

		results := make([]*models.Diff, len(diff))
		for i, d := range diff {
			results[i] = transformDifferenceToDiff(d)
		}
		var nextOffset string
		if hasMore && len(diff) > 0 {
			nextOffset = diff[len(diff)-1].Path
		}
		return branches.NewDiffBranchOK().WithPayload(&branches.DiffBranchOKBody{
			Results: results,
			Pagination: &models.Pagination{
				NextOffset: nextOffset,
				HasMore:    swag.Bool(hasMore),
				Results:    swag.Int64(int64(len(diff))),
				MaxPerPage: swag.Int64(MaxResultsPerPage),
			},
		})
	})
}

func (c *Controller) RefsDiffRefsHandler() refs.DiffRefsHandler {
	return refs.DiffRefsHandlerFunc(func(params refs.DiffRefsParams, user *models.User) middleware.Responder {
		ctx, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.ListObjectsAction,
				Resource: permissions.RepoArn(params.Repository),
			},
		})
		if err != nil {
			return refs.NewDiffRefsUnauthorized().WithPayload(responseErrorFrom(err))
		}
		c.LogAction(ctx, "diff_refs")
		limit := int(swag.Int64Value(params.Amount))
		after := swag.StringValue(params.After)
		diffFunc := c.Catalog.Compare // default diff type is three-dot
		if swag.StringValue(params.Type) == "two_dot" {
			diffFunc = c.Catalog.Diff
		}
		diff, hasMore, err := diffFunc(ctx, params.Repository, params.LeftRef, params.RightRef, catalog.DiffParams{
			Limit: limit,
			After: after,
		})
		if errors.Is(err, catalog.ErrFeatureNotSupported) {
			return refs.NewDiffRefsDefault(http.StatusNotImplemented).WithPayload(responseError(err.Error()))
		}
		if errors.Is(err, catalog.ErrNotFound) {
			return refs.NewDiffRefsNotFound().WithPayload(responseError(err.Error()))
		}
		if err != nil {
			return refs.NewDiffRefsDefault(http.StatusInternalServerError).
				WithPayload(responseError("could not diff references: %s", err))
		}

		results := make([]*models.Diff, len(diff))
		for i, d := range diff {
			results[i] = transformDifferenceToDiff(d)
		}
		var nextOffset string
		if hasMore && len(diff) > 0 {
			nextOffset = diff[len(diff)-1].Path
		}
		return refs.NewDiffRefsOK().WithPayload(&refs.DiffRefsOKBody{
			Results: results,
			Pagination: &models.Pagination{
				NextOffset: nextOffset,
				HasMore:    swag.Bool(hasMore),
				Results:    swag.Int64(int64(len(diff))),
				MaxPerPage: swag.Int64(MaxResultsPerPage),
			},
		})
	})
}

func (c *Controller) ObjectsStatObjectHandler() objects.StatObjectHandler {
	return objects.StatObjectHandlerFunc(func(params objects.StatObjectParams, user *models.User) middleware.Responder {
		ctx, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.ReadObjectAction,
				Resource: permissions.ObjectArn(params.Repository, params.Path),
			},
		})
		if err != nil {
			return objects.NewStatObjectUnauthorized().WithPayload(responseErrorFrom(err))
		}
		c.LogAction(ctx, "stat_object")

		entry, err := c.Catalog.GetEntry(ctx, params.Repository, params.Ref, params.Path, catalog.GetEntryParams{ReturnExpired: true})
		if errors.Is(err, catalog.ErrNotFound) {
			return objects.NewStatObjectNotFound().WithPayload(responseError("resource not found"))
		}
		if err != nil {
			return objects.NewStatObjectDefault(http.StatusInternalServerError).WithPayload(responseErrorFrom(err))
		}

		repo, err := c.Catalog.GetRepository(ctx, params.Repository)
		if err != nil {
			return objects.NewStatObjectDefault(http.StatusInternalServerError).WithPayload(responseErrorFrom(err))
		}

		qk, err := block.ResolveNamespace(repo.StorageNamespace, entry.PhysicalAddress)
		if err != nil {
			return objects.NewStatObjectDefault(http.StatusInternalServerError).WithPayload(responseErrorFrom(err))
		}

		// serialize entry
		obj := &models.ObjectStats{
			Checksum:        entry.Checksum,
			Mtime:           entry.CreationDate.Unix(),
			Path:            params.Path,
			PhysicalAddress: qk.Format(),
			PathType:        models.ObjectStatsPathTypeObject,
			SizeBytes:       swag.Int64(entry.Size),
		}

		if entry.Expired {
			return objects.NewStatObjectGone().WithPayload(obj)
		}
		return objects.NewStatObjectOK().WithPayload(obj)
	})
}

func (c *Controller) ObjectsGetUnderlyingPropertiesHandler() objects.GetUnderlyingPropertiesHandler {
	return objects.GetUnderlyingPropertiesHandlerFunc(func(params objects.GetUnderlyingPropertiesParams, user *models.User) middleware.Responder {
		ctx, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.ReadObjectAction,
				Resource: permissions.ObjectArn(params.Repository, params.Path),
			},
		})
		if err != nil {
			return objects.NewGetUnderlyingPropertiesUnauthorized().WithPayload(responseErrorFrom(err))
		}
		c.LogAction(ctx, "object_underlying_properties")

		// read repo
		repo, err := c.Catalog.GetRepository(ctx, params.Repository)
		if errors.Is(err, catalog.ErrNotFound) {
			return objects.NewGetUnderlyingPropertiesNotFound().WithPayload(responseError("resource not found"))
		}
		if err != nil {
			return objects.NewGetUnderlyingPropertiesDefault(http.StatusInternalServerError).WithPayload(responseErrorFrom(err))
		}

		entry, err := c.Catalog.GetEntry(ctx, params.Repository, params.Ref, params.Path, catalog.GetEntryParams{})
		if errors.Is(err, catalog.ErrNotFound) {
			return objects.NewGetUnderlyingPropertiesNotFound().WithPayload(responseError("resource not found"))
		}
		if err != nil {
			return objects.NewGetUnderlyingPropertiesDefault(http.StatusInternalServerError).WithPayload(responseErrorFrom(err))
		}

		// read object properties from underlying storage
		properties, err := c.BlockAdapter.GetProperties(ctx, block.ObjectPointer{StorageNamespace: repo.StorageNamespace, Identifier: entry.PhysicalAddress})
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
		ctx, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.ReadObjectAction,
				Resource: permissions.ObjectArn(params.Repository, params.Path),
			},
		})
		if err != nil {
			return objects.NewGetObjectUnauthorized().WithPayload(responseErrorFrom(err))
		}
		c.LogAction(ctx, "get_object")

		// read repo
		repo, err := c.Catalog.GetRepository(ctx, params.Repository)
		if errors.Is(err, catalog.ErrNotFound) {
			return objects.NewGetObjectNotFound().WithPayload(responseError("resource not found"))
		}
		if err != nil {
			return objects.NewGetObjectDefault(http.StatusInternalServerError).WithPayload(responseErrorFrom(err))
		}

		// read the FS entry
		entry, err := c.Catalog.GetEntry(ctx, params.Repository, params.Ref, params.Path, catalog.GetEntryParams{ReturnExpired: true})
		if errors.Is(err, catalog.ErrNotFound) {
			return objects.NewGetObjectNotFound().WithPayload(responseError("resource not found"))
		}
		if entry.Expired {
			return objects.NewGetObjectGone().WithPayload(responseError("resource expired"))
		}
		if err != nil {
			return objects.NewGetObjectDefault(http.StatusInternalServerError).WithPayload(responseErrorFrom(err))
		}
		// setup response
		res := objects.NewGetObjectOK()
		res.ETag = httputil.ETag(entry.Checksum)
		res.LastModified = httputil.HeaderTimestamp(entry.CreationDate)
		res.ContentDisposition = fmt.Sprintf("filename=\"%s\"", filepath.Base(entry.Path))

		// build a response as a multi-reader
		res.ContentLength = entry.Size
		reader, err := c.BlockAdapter.Get(ctx, block.ObjectPointer{StorageNamespace: repo.StorageNamespace, Identifier: entry.PhysicalAddress}, entry.Size)
		if err != nil {
			return objects.NewGetObjectDefault(http.StatusInternalServerError).WithPayload(responseErrorFrom(err))
		}

		// done
		res.Payload = reader
		return res
	})
}
func (c *Controller) ConfigGetConfigHandler() configop.GetConfigHandler {
	return configop.GetConfigHandlerFunc(func(params configop.GetConfigParams, user *models.User) middleware.Responder {
		_, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.ReadConfigAction,
				Resource: permissions.All,
			},
		})

		if err != nil {
			return configop.NewGetConfigUnauthorized().WithPayload(responseErrorFrom(err))
		}
		storageNamespaceInfo := c.BlockAdapter.GetStorageNamespaceInfo()
		return configop.NewGetConfigOK().WithPayload(&models.Config{
			BlockstoreNamespaceValidityRegex: storageNamespaceInfo.ValidityRegex,
			BlockstoreNamespaceExample:       storageNamespaceInfo.Example,
		})
	})
}

func (c *Controller) MetadataCreateSymlinkHandler() metadata.CreateSymlinkHandler {
	return metadata.CreateSymlinkHandlerFunc(func(params metadata.CreateSymlinkParams, user *models.User) middleware.Responder {
		ctx, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.WriteObjectAction,
				Resource: permissions.ObjectArn(params.Repository, params.Branch),
			},
		})
		if err != nil {
			return metadata.NewCreateSymlinkUnauthorized().WithPayload(responseErrorFrom(err))
		}
		c.LogAction(ctx, "create_symlink")

		// read repo
		repo, err := c.Catalog.GetRepository(ctx, params.Repository)
		if errors.Is(err, catalog.ErrNotFound) {
			return metadata.NewCreateSymlinkNotFound().WithPayload(responseError("resource not found"))
		}
		if err != nil {
			return metadata.NewCreateSymlinkDefault(http.StatusInternalServerError).WithPayload(responseErrorFrom(err))
		}
		// list entries
		var currentPath string
		var currentAddresses []string
		var after string
		var entries []*catalog.DBEntry
		hasMore := true
		for hasMore {
			entries, hasMore, err = c.Catalog.ListEntries(
				ctx,
				params.Repository,
				params.Branch,
				swag.StringValue(params.Location),
				after,
				"",
				-1)
			if errors.Is(err, catalog.ErrNotFound) {
				return metadata.NewCreateSymlinkNotFound().WithPayload(responseError("could not find requested path"))
			}
			if err != nil {
				return metadata.NewCreateSymlinkDefault(http.StatusInternalServerError).
					WithPayload(responseError("error while listing objects: %s", err))
			}
			// loop all entries enter to map[path] physicalAddress
			for _, entry := range entries {
				address := fmt.Sprintf("%s/%s", repo.StorageNamespace, entry.PhysicalAddress)
				var path string
				idx := strings.LastIndex(entry.Path, "/")
				if idx != -1 {
					path = entry.Path[0:idx]
				}
				if path != currentPath {
					// push current
					err := writeSymlink(ctx, params, repo, path, currentAddresses, c.BlockAdapter)
					if err != nil {
						return metadata.NewCreateSymlinkDefault(http.StatusInternalServerError).
							WithPayload(responseError("error while writing symlinks: %s", err))
					}
					currentPath = path
					currentAddresses = []string{address}
				} else {
					currentAddresses = append(currentAddresses, address)
				}
			}
			after = entries[len(entries)-1].Path
		}
		if len(currentAddresses) > 0 {
			err = writeSymlink(ctx, params, repo, currentPath, currentAddresses, c.BlockAdapter)
			if err != nil {
				return metadata.NewCreateSymlinkDefault(http.StatusInternalServerError).
					WithPayload(responseError("error while writing symlinks: %s", err))
			}
		}
		metaLocation := fmt.Sprintf("%s/%s", repo.StorageNamespace, lakeFSPrefix)
		return metadata.NewCreateSymlinkCreated().WithPayload(metaLocation)
	})
}
func writeSymlink(ctx context.Context, params metadata.CreateSymlinkParams, repo *catalog.Repository, path string, addresses []string, adapter block.Adapter) error {
	address := fmt.Sprintf("%s/%s/%s/%s/symlink.txt", lakeFSPrefix, repo.Name, params.Branch, path)
	data := strings.Join(addresses, "\n")
	symlinkReader := aws.ReadSeekCloser(strings.NewReader(data))
	err := adapter.Put(ctx, block.ObjectPointer{
		StorageNamespace: repo.StorageNamespace,
		Identifier:       address,
	}, int64(len(data)), symlinkReader, block.PutOpts{})

	return err
}

func (c *Controller) MetadataGetMetarangeHandler() metadata.GetMetaRangeHandler {
	return metadata.GetMetaRangeHandlerFunc(func(params metadata.GetMetaRangeParams, user *models.User) middleware.Responder {
		ctx, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.ListObjectsAction,
				Resource: permissions.RepoArn(params.Repository),
			},
			{
				Action:   permissions.ReadRepositoryAction,
				Resource: permissions.RepoArn(params.Repository),
			},
		})
		if err != nil {
			return metadata.NewGetMetaRangeUnauthorized().WithPayload(responseErrorFrom(err))
		}
		c.LogAction(ctx, "metadata_get_metarange")

		metarange, err := c.Catalog.GetMetaRange(ctx, params.Repository, params.MetaRange)
		if err != nil {
			return metadata.NewGetMetaRangeDefault(http.StatusInternalServerError).WithPayload(responseErrorFrom(err))
		}
		ret := metadata.NewGetMetaRangeOK()
		ret.Location = metarange.Address
		return ret
	})
}

func (c *Controller) MetadataGetRangeHandler() metadata.GetRangeHandler {
	return metadata.GetRangeHandlerFunc(func(params metadata.GetRangeParams, user *models.User) middleware.Responder {
		ctx, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.ListObjectsAction,
				Resource: permissions.RepoArn(params.Repository),
			},
			{
				Action:   permissions.ReadRepositoryAction,
				Resource: permissions.RepoArn(params.Repository),
			},
		})
		if err != nil {
			return metadata.NewGetRangeUnauthorized().WithPayload(responseErrorFrom(err))
		}
		c.LogAction(ctx, "metadata_get_range")

		rng, err := c.Catalog.GetRange(ctx, params.Repository, params.Range)
		if err != nil {
			return metadata.NewGetRangeDefault(http.StatusInternalServerError).WithPayload(responseErrorFrom(err))
		}
		ret := metadata.NewGetRangeOK()
		ret.Location = rng.Address
		return ret
	})
}

func (c *Controller) ObjectsListObjectsHandler() objects.ListObjectsHandler {
	return objects.ListObjectsHandlerFunc(func(params objects.ListObjectsParams, user *models.User) middleware.Responder {
		ctx, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.ListObjectsAction,
				Resource: permissions.RepoArn(params.Repository),
			},
		})
		if err != nil {
			return objects.NewListObjectsUnauthorized().WithPayload(responseErrorFrom(err))
		}
		c.LogAction(ctx, "list_objects")

		after, amount := getPaginationParams(params.After, params.Amount)

		// discern between an empty delimiter and no delimiter being passed at all
		// by default, go-swagger will use the default value ("/") even if we pass
		// a delimiter param that is explicitly empty. This overrides this (wrong) behavior.
		delimiter := params.Delimiter
		query := params.HTTPRequest.URL.Query()
		_, delimiterPassed := query["delimiter"]
		queryDelimiter := query.Get("delimiter")
		if delimiterPassed && queryDelimiter == "" {
			delimiter = ""
		}

		res, hasMore, err := c.Catalog.ListEntries(
			ctx,
			params.Repository,
			params.Ref,
			swag.StringValue(params.Prefix),
			after,
			delimiter,
			amount)
		if errors.Is(err, catalog.ErrNotFound) {
			return objects.NewListObjectsNotFound().WithPayload(responseError("could not find requested path"))
		}
		if err != nil {
			return objects.NewListObjectsDefault(http.StatusInternalServerError).
				WithPayload(responseError("error while listing objects: %s", err))
		}

		repo, err := c.Catalog.GetRepository(ctx, params.Repository)
		if err != nil {
			return objects.NewStatObjectDefault(http.StatusInternalServerError).WithPayload(responseErrorFrom(err))
		}

		objList := make([]*models.ObjectStats, len(res))
		var lastID string
		for i, entry := range res {
			qk, err := block.ResolveNamespace(repo.StorageNamespace, entry.PhysicalAddress)
			if err != nil {
				return objects.NewStatObjectDefault(http.StatusInternalServerError).WithPayload(responseErrorFrom(err))
			}

			if entry.CommonLevel {
				objList[i] = &models.ObjectStats{
					Path:     entry.Path,
					PathType: models.ObjectStatsPathTypeCommonPrefix,
				}
			} else {
				var mtime int64
				if !entry.CreationDate.IsZero() {
					mtime = entry.CreationDate.Unix()
				}
				objList[i] = &models.ObjectStats{
					Checksum:        entry.Checksum,
					Mtime:           mtime,
					Path:            entry.Path,
					PhysicalAddress: qk.Format(),
					PathType:        models.ObjectStatsPathTypeObject,
					SizeBytes:       swag.Int64(entry.Size),
				}
			}
			lastID = entry.Path
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
			returnValue.Payload.Pagination.NextOffset = lastID
		}
		return returnValue
	})
}

func (c *Controller) ObjectsStageObjectHandler() objects.StageObjectHandler {
	return objects.StageObjectHandlerFunc(func(params objects.StageObjectParams, user *models.User) middleware.Responder {
		ctx, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.WriteObjectAction,
				Resource: permissions.ObjectArn(params.Repository, params.Path),
			},
		})
		if err != nil {
			return objects.NewStageObjectUnauthorized().WithPayload(responseErrorFrom(err))
		}
		c.LogAction(ctx, "stage_object")

		repo, err := c.Catalog.GetRepository(ctx, params.Repository)
		if errors.Is(err, catalog.ErrNotFound) {
			return objects.NewStageObjectNotFound().WithPayload(responseErrorFrom(err))
		} else if err != nil {
			return objects.NewStageObjectDefault(http.StatusInternalServerError).WithPayload(responseErrorFrom(err))
		}

		// write metadata
		qk, err := block.ResolveNamespace(repo.StorageNamespace, swag.StringValue(params.Object.PhysicalAddress))
		if err != nil {
			return objects.NewStageObjectDefault(http.StatusInternalServerError).WithPayload(responseErrorFrom(err))
		}

		blockStoreType := c.BlockAdapter.BlockstoreType()
		if qk.StorageType.String() != blockStoreType {
			return objects.NewStageObjectBadRequest().WithPayload(
				responseError("invalid storage type: %s: current block adapter is %s",
					qk.StorageType.String(),
					blockStoreType,
				))
		}

		writeTime := time.Now()
		entry := catalog.DBEntry{
			CommonLevel:     false,
			Path:            params.Path,
			PhysicalAddress: swag.StringValue(params.Object.PhysicalAddress),
			CreationDate:    writeTime,
			Size:            swag.Int64Value(params.Object.SizeBytes),
			Checksum:        swag.StringValue(params.Object.Checksum),
		}
		if len(params.Object.Metadata) > 0 {
			entry.Metadata = params.Object.Metadata
		}

		err = c.Catalog.CreateEntry(ctx, repo.Name, params.Branch, entry)
		if errors.Is(err, catalog.ErrNotFound) {
			return objects.NewStageObjectNotFound().WithPayload(responseErrorFrom(err))
		}
		if err != nil {
			return objects.NewStageObjectDefault(http.StatusInternalServerError).WithPayload(responseErrorFrom(err))
		}

		return objects.NewStageObjectCreated().WithPayload(&models.ObjectStats{
			Checksum:        entry.Checksum,
			Mtime:           entry.CreationDate.Unix(),
			Path:            entry.Path,
			PhysicalAddress: qk.Format(),
			PathType:        models.ObjectStatsPathTypeObject,
			SizeBytes:       swag.Int64(entry.Size),
		})
	})
}

func (c *Controller) ObjectsUploadObjectHandler() objects.UploadObjectHandler {
	return objects.UploadObjectHandlerFunc(func(params objects.UploadObjectParams, user *models.User) middleware.Responder {
		ctx, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.WriteObjectAction,
				Resource: permissions.ObjectArn(params.Repository, params.Path),
			},
		})
		if err != nil {
			return objects.NewUploadObjectUnauthorized().WithPayload(responseErrorFrom(err))
		}
		c.LogAction(ctx, "put_object")

		repo, err := c.Catalog.GetRepository(ctx, params.Repository)
		if errors.Is(err, catalog.ErrNotFound) {
			return objects.NewUploadObjectNotFound().WithPayload(responseError("repository not found"))
		}
		if err != nil {
			return objects.NewUploadObjectDefault(http.StatusInternalServerError).WithPayload(responseErrorFrom(err))
		}
		// check if branch exists - it is still a possibility, but we don't want to upload large object when the branch was not there in the first place
		branchExists, err := c.Catalog.BranchExists(ctx, params.Repository, params.Branch)
		if err != nil {
			return objects.NewUploadObjectDefault(http.StatusInternalServerError).WithPayload(responseErrorFrom(err))
		}
		if !branchExists {
			return objects.NewUploadObjectNotFound().WithPayload(responseError("branch '%s' not found", params.Branch))
		}
		// workaround in order to extract file content-length using swagger
		file, ok := params.Content.(*runtime.File)
		if !ok {
			return objects.NewUploadObjectDefault(http.StatusInternalServerError).WithPayload(responseError("failed extracting size from file"))
		}
		byteSize := file.Header.Size

		// write the content
		blob, err := upload.WriteBlob(ctx, c.BlockAdapter, repo.StorageNamespace, params.Content, byteSize, block.PutOpts{StorageClass: params.StorageClass})
		if err != nil {
			return objects.NewUploadObjectDefault(http.StatusInternalServerError).WithPayload(responseErrorFrom(err))
		}

		// write metadata
		writeTime := time.Now()
		entry := catalog.DBEntry{
			Path:            params.Path,
			PhysicalAddress: blob.PhysicalAddress,
			CreationDate:    writeTime,
			Size:            blob.Size,
			Checksum:        blob.Checksum,
		}
		err = c.Catalog.CreateEntry(ctx, repo.Name, params.Branch, entry)
		if errors.Is(err, catalog.ErrNotFound) {
			return objects.NewUploadObjectNotFound().WithPayload(responseErrorFrom(err))
		}
		if err != nil {
			return objects.NewUploadObjectDefault(http.StatusInternalServerError).WithPayload(responseErrorFrom(err))
		}

		qk, err := block.ResolveNamespace(repo.StorageNamespace, blob.PhysicalAddress)
		if err != nil {
			return objects.NewUploadObjectDefault(http.StatusInternalServerError).WithPayload(responseErrorFrom(err))
		}

		return objects.NewUploadObjectCreated().WithPayload(&models.ObjectStats{
			Checksum:        blob.Checksum,
			Mtime:           writeTime.Unix(),
			Path:            params.Path,
			PhysicalAddress: qk.Format(),
			PathType:        models.ObjectStatsPathTypeObject,
			SizeBytes:       swag.Int64(blob.Size),
		})
	})
}

func (c *Controller) ObjectsDeleteObjectHandler() objects.DeleteObjectHandler {
	return objects.DeleteObjectHandlerFunc(func(params objects.DeleteObjectParams, user *models.User) middleware.Responder {
		ctx, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.DeleteObjectAction,
				Resource: permissions.ObjectArn(params.Repository, params.Path),
			},
		})
		if err != nil {
			return objects.NewDeleteObjectUnauthorized().WithPayload(responseErrorFrom(err))
		}
		c.LogAction(ctx, "delete_object")

		err = c.Catalog.DeleteEntry(ctx, params.Repository, params.Branch, params.Path)
		if errors.Is(err, catalog.ErrNotFound) {
			return objects.NewDeleteObjectNotFound().WithPayload(responseError("resource not found"))
		}
		if err != nil {
			return objects.NewDeleteObjectDefault(http.StatusInternalServerError).WithPayload(responseErrorFrom(err))
		}

		return objects.NewDeleteObjectNoContent()
	})
}

func (c *Controller) RevertHandler() branches.RevertHandler {
	return branches.RevertHandlerFunc(func(params branches.RevertParams, user *models.User) middleware.Responder {
		ctx, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.RevertBranchAction,
				Resource: permissions.BranchArn(params.Repository, params.Branch),
			},
		})
		if err != nil {
			return branches.NewRevertUnauthorized().WithPayload(responseErrorFrom(err))
		}
		c.LogAction(ctx, "revert_branch")
		userModel, err := c.Auth.GetUser(ctx, user.ID)
		if err != nil {
			return branches.NewRevertUnauthorized().WithPayload(responseErrorFrom(err))
		}
		committer := userModel.Username
		err = c.Catalog.Revert(ctx, params.Repository, params.Branch, catalog.RevertParams{
			Reference:    params.Revert.Ref,
			Committer:    committer,
			ParentNumber: int(params.Revert.ParentNumber),
		})
		if errors.Is(err, graveler.ErrNotFound) {
			return branches.NewRevertNotFound().WithPayload(responseErrorFrom(err))
		}
		if err != nil {
			return branches.NewRevertDefault(http.StatusInternalServerError).WithPayload(responseErrorFrom(err))
		}
		return branches.NewRevertNoContent()
	})
}

func (c *Controller) ResetBranchHandler() branches.ResetBranchHandler {
	return branches.ResetBranchHandlerFunc(func(params branches.ResetBranchParams, user *models.User) middleware.Responder {
		ctx, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.RevertBranchAction,
				Resource: permissions.BranchArn(params.Repository, params.Branch),
			},
		})
		if err != nil {
			return branches.NewResetBranchUnauthorized().WithPayload(responseErrorFrom(err))
		}
		c.LogAction(ctx, "reset_branch")
		switch swag.StringValue(params.Reset.Type) {
		case models.ResetCreationTypeCommit:
			err = c.Catalog.RollbackCommit(ctx, params.Repository, params.Branch, params.Reset.Commit)
		case models.ResetCreationTypeCommonPrefix:
			err = c.Catalog.ResetEntries(ctx, params.Repository, params.Branch, params.Reset.Path)
		case models.ResetCreationTypeReset:
			err = c.Catalog.ResetBranch(ctx, params.Repository, params.Branch)
		case models.ResetCreationTypeObject:
			err = c.Catalog.ResetEntry(ctx, params.Repository, params.Branch, params.Reset.Path)
		default:
			return branches.NewResetBranchNotFound().
				WithPayload(responseError("reset type not found"))
		}
		if errors.Is(err, catalog.ErrNotFound) {
			return branches.NewResetBranchNotFound().WithPayload(responseErrorFrom(err))
		}
		if err != nil {
			return branches.NewResetBranchDefault(http.StatusInternalServerError).WithPayload(responseErrorFrom(err))
		}

		return branches.NewResetBranchNoContent()
	})
}

func (c *Controller) CreateUserHandler() authop.CreateUserHandler {
	return authop.CreateUserHandlerFunc(func(params authop.CreateUserParams, user *models.User) middleware.Responder {
		ctx, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.CreateUserAction,
				Resource: permissions.UserArn(swag.StringValue(params.User.ID)),
			},
		})
		if err != nil {
			return authop.NewCreateUserUnauthorized().
				WithPayload(responseErrorFrom(err))
		}
		u := &model.User{
			CreatedAt: time.Now(),
			Username:  swag.StringValue(params.User.ID),
		}
		err = c.Auth.CreateUser(ctx, u)
		c.LogAction(ctx, "create_user")
		if err != nil {
			return authop.NewCreateUserDefault(http.StatusInternalServerError).
				WithPayload(responseErrorFrom(err))
		}

		return authop.NewCreateUserCreated().
			WithPayload(&models.User{
				CreationDate: u.CreatedAt.Unix(),
				ID:           u.Username,
			})
	})
}

func (c *Controller) ListUsersHandler() authop.ListUsersHandler {
	return authop.ListUsersHandlerFunc(func(params authop.ListUsersParams, user *models.User) middleware.Responder {
		ctx, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.ListUsersAction,
				Resource: permissions.All,
			},
		})
		if err != nil {
			return authop.NewListUsersUnauthorized().
				WithPayload(responseErrorFrom(err))
		}

		c.LogAction(ctx, "list_users")
		users, paginator, err := c.Auth.ListUsers(ctx, &model.PaginationParams{
			After:  swag.StringValue(params.After),
			Amount: pageAmount(params.Amount),
		})
		if err != nil {
			return authop.NewListUsersDefault(http.StatusInternalServerError).
				WithPayload(responseErrorFrom(err))
		}

		response := make([]*models.User, len(users))
		for i, u := range users {
			response[i] = &models.User{
				CreationDate: u.CreatedAt.Unix(),
				ID:           u.Username,
			}
		}

		return authop.NewListUsersOK().
			WithPayload(&authop.ListUsersOKBody{
				Pagination: createPaginator(paginator.NextPageToken, len(response)),
				Results:    response,
			})
	})
}

func (c *Controller) GetUserHandler() authop.GetUserHandler {
	return authop.GetUserHandlerFunc(func(params authop.GetUserParams, user *models.User) middleware.Responder {
		ctx, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.ReadUserAction,
				Resource: permissions.UserArn(params.UserID),
			},
		})
		if err != nil {
			return authop.NewGetUserUnauthorized().
				WithPayload(responseErrorFrom(err))
		}
		c.LogAction(ctx, "get_user")
		u, err := c.Auth.GetUser(ctx, params.UserID)
		if errors.Is(err, auth.ErrNotFound) {
			return authop.NewGetUserNotFound().
				WithPayload(responseError("user not found"))
		}
		if err != nil {
			return authop.NewGetUserDefault(http.StatusInternalServerError).
				WithPayload(responseErrorFrom(err))
		}

		return authop.NewGetUserOK().
			WithPayload(&models.User{
				CreationDate: u.CreatedAt.Unix(),
				ID:           u.Username,
			})
	})
}

func (c *Controller) DeleteUserHandler() authop.DeleteUserHandler {
	return authop.DeleteUserHandlerFunc(func(params authop.DeleteUserParams, user *models.User) middleware.Responder {
		ctx, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.DeleteUserAction,
				Resource: permissions.UserArn(params.UserID),
			},
		})
		if err != nil {
			return authop.NewDeleteUserUnauthorized().
				WithPayload(responseErrorFrom(err))
		}

		c.LogAction(ctx, "delete_user")
		err = c.Auth.DeleteUser(ctx, params.UserID)
		if errors.Is(err, auth.ErrNotFound) {
			return authop.NewDeleteUserNotFound().
				WithPayload(responseError("user not found"))
		}
		if err != nil {
			return authop.NewDeleteUserDefault(http.StatusInternalServerError).
				WithPayload(responseErrorFrom(err))
		}

		return authop.NewDeleteUserNoContent()
	})
}

func (c *Controller) GetGroupHandler() authop.GetGroupHandler {
	return authop.GetGroupHandlerFunc(func(params authop.GetGroupParams, user *models.User) middleware.Responder {
		ctx, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.ReadGroupAction,
				Resource: permissions.GroupArn(params.GroupID),
			},
		})
		if err != nil {
			return authop.NewGetGroupUnauthorized().
				WithPayload(responseErrorFrom(err))
		}
		c.LogAction(ctx, "get_group")
		g, err := c.Auth.GetGroup(ctx, params.GroupID)
		if errors.Is(err, auth.ErrNotFound) {
			return authop.NewGetGroupNotFound().
				WithPayload(responseError("group not found"))
		}
		if err != nil {
			return authop.NewGetGroupDefault(http.StatusInternalServerError).
				WithPayload(responseErrorFrom(err))
		}

		return authop.NewGetGroupOK().
			WithPayload(&models.Group{
				CreationDate: g.CreatedAt.Unix(),
				ID:           g.DisplayName,
			})
	})
}

func (c *Controller) ListGroupsHandler() authop.ListGroupsHandler {
	return authop.ListGroupsHandlerFunc(func(params authop.ListGroupsParams, user *models.User) middleware.Responder {
		ctx, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.ListGroupsAction,
				Resource: permissions.All,
			},
		})
		if err != nil {
			return authop.NewListGroupsUnauthorized().
				WithPayload(responseErrorFrom(err))
		}

		c.LogAction(ctx, "list_groups")
		groups, paginator, err := c.Auth.ListGroups(ctx, &model.PaginationParams{
			After:  swag.StringValue(params.After),
			Amount: pageAmount(params.Amount),
		})

		if err != nil {
			return authop.NewListGroupsDefault(http.StatusInternalServerError).
				WithPayload(responseErrorFrom(err))
		}

		response := make([]*models.Group, len(groups))
		for i, g := range groups {
			response[i] = &models.Group{
				CreationDate: g.CreatedAt.Unix(),
				ID:           g.DisplayName,
			}
		}

		return authop.NewListGroupsOK().
			WithPayload(&authop.ListGroupsOKBody{
				Pagination: createPaginator(paginator.NextPageToken, len(response)),
				Results:    response,
			})
	})
}

func (c *Controller) CreateGroupHandler() authop.CreateGroupHandler {
	return authop.CreateGroupHandlerFunc(func(params authop.CreateGroupParams, user *models.User) middleware.Responder {
		ctx, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.CreateGroupAction,
				Resource: permissions.GroupArn(swag.StringValue(params.Group.ID)),
			},
		})
		if err != nil {
			return authop.NewCreateGroupUnauthorized().
				WithPayload(responseErrorFrom(err))
		}
		g := &model.Group{
			CreatedAt:   time.Now(),
			DisplayName: swag.StringValue(params.Group.ID),
		}

		c.LogAction(ctx, "create_group")
		err = c.Auth.CreateGroup(ctx, g)
		if err != nil {
			return authop.NewCreateGroupDefault(http.StatusInternalServerError).
				WithPayload(responseErrorFrom(err))
		}

		return authop.NewCreateGroupCreated().
			WithPayload(&models.Group{
				CreationDate: g.CreatedAt.Unix(),
				ID:           g.DisplayName,
			})
	})
}

func (c *Controller) DeleteGroupHandler() authop.DeleteGroupHandler {
	return authop.DeleteGroupHandlerFunc(func(params authop.DeleteGroupParams, user *models.User) middleware.Responder {
		ctx, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.DeleteGroupAction,
				Resource: permissions.GroupArn(params.GroupID),
			},
		})
		if err != nil {
			return authop.NewDeleteGroupUnauthorized().
				WithPayload(responseErrorFrom(err))
		}

		c.LogAction(ctx, "delete_group")
		err = c.Auth.DeleteGroup(ctx, params.GroupID)
		if errors.Is(err, auth.ErrNotFound) {
			return authop.NewDeleteGroupNotFound().
				WithPayload(responseError("group not found"))
		}
		if err != nil {
			return authop.NewDeleteGroupDefault(http.StatusInternalServerError).
				WithPayload(responseErrorFrom(err))
		}
		return authop.NewDeleteGroupNoContent()
	})
}

func serializePolicy(p *model.Policy) *models.Policy {
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

func (c *Controller) ListPoliciesHandler() authop.ListPoliciesHandler {
	return authop.ListPoliciesHandlerFunc(func(params authop.ListPoliciesParams, user *models.User) middleware.Responder {
		ctx, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.ListPoliciesAction,
				Resource: permissions.All,
			},
		})
		if err != nil {
			return authop.NewListPoliciesUnauthorized().
				WithPayload(responseErrorFrom(err))
		}

		c.LogAction(ctx, "list_policies")
		policies, paginator, err := c.Auth.ListPolicies(ctx, &model.PaginationParams{
			After:  swag.StringValue(params.After),
			Amount: pageAmount(params.Amount),
		})
		if err != nil {
			return authop.NewListPoliciesDefault(http.StatusInternalServerError).
				WithPayload(responseErrorFrom(err))
		}

		response := make([]*models.Policy, len(policies))
		for i, p := range policies {
			response[i] = serializePolicy(p)
		}

		return authop.NewListPoliciesOK().
			WithPayload(&authop.ListPoliciesOKBody{
				Pagination: createPaginator(paginator.NextPageToken, len(response)),
				Results:    response,
			})
	})
}

func (c *Controller) CreatePolicyHandler() authop.CreatePolicyHandler {
	return authop.CreatePolicyHandlerFunc(func(params authop.CreatePolicyParams, user *models.User) middleware.Responder {
		ctx, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.CreatePolicyAction,
				Resource: permissions.PolicyArn(swag.StringValue(params.Policy.ID)),
			},
		})
		if err != nil {
			return authop.NewCreatePolicyUnauthorized().
				WithPayload(responseErrorFrom(err))
		}

		stmts := make(model.Statements, len(params.Policy.Statement))
		for i, apiStatement := range params.Policy.Statement {
			stmts[i] = model.Statement{
				Effect:   swag.StringValue(apiStatement.Effect),
				Action:   apiStatement.Action,
				Resource: swag.StringValue(apiStatement.Resource),
			}
		}

		p := &model.Policy{
			CreatedAt:   time.Now(),
			DisplayName: swag.StringValue(params.Policy.ID),
			Statement:   stmts,
		}

		c.LogAction(ctx, "create_policy")
		err = c.Auth.WritePolicy(ctx, p)
		if err != nil {
			return authop.NewCreatePolicyDefault(http.StatusInternalServerError).
				WithPayload(responseErrorFrom(err))
		}

		return authop.NewCreatePolicyCreated().
			WithPayload(serializePolicy(p))
	})
}

func (c *Controller) GetPolicyHandler() authop.GetPolicyHandler {
	return authop.GetPolicyHandlerFunc(func(params authop.GetPolicyParams, user *models.User) middleware.Responder {
		ctx, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.ReadPolicyAction,
				Resource: permissions.PolicyArn(params.PolicyID),
			},
		})
		if err != nil {
			return authop.NewGetPolicyUnauthorized().
				WithPayload(responseErrorFrom(err))
		}
		c.LogAction(ctx, "get_policy")
		p, err := c.Auth.GetPolicy(ctx, params.PolicyID)
		if errors.Is(err, auth.ErrNotFound) {
			return authop.NewGetPolicyNotFound().
				WithPayload(responseError("policy not found"))
		}
		if err != nil {
			return authop.NewGetPolicyDefault(http.StatusInternalServerError).
				WithPayload(responseErrorFrom(err))
		}

		return authop.NewGetPolicyOK().
			WithPayload(serializePolicy(p))
	})
}

func (c *Controller) UpdatePolicyHandler() authop.UpdatePolicyHandler {
	return authop.UpdatePolicyHandlerFunc(func(params authop.UpdatePolicyParams, user *models.User) middleware.Responder {
		ctx, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.UpdatePolicyAction,
				Resource: permissions.PolicyArn(params.PolicyID),
			},
		})
		if err != nil {
			return authop.NewUpdatePolicyUnauthorized().
				WithPayload(responseErrorFrom(err))
		}

		stmts := make(model.Statements, len(params.Policy.Statement))
		for i, apiStatement := range params.Policy.Statement {
			stmts[i] = model.Statement{
				Effect:   swag.StringValue(apiStatement.Effect),
				Action:   apiStatement.Action,
				Resource: swag.StringValue(apiStatement.Resource),
			}
		}

		p := &model.Policy{
			CreatedAt:   time.Now(),
			DisplayName: swag.StringValue(params.Policy.ID),
			Statement:   stmts,
		}

		c.LogAction(ctx, "update_policy")
		err = c.Auth.WritePolicy(ctx, p)
		if err != nil {
			return authop.NewUpdatePolicyDefault(http.StatusInternalServerError).
				WithPayload(responseErrorFrom(err))
		}

		return authop.NewUpdatePolicyOK().
			WithPayload(serializePolicy(p))
	})
}

func (c *Controller) DeletePolicyHandler() authop.DeletePolicyHandler {
	return authop.DeletePolicyHandlerFunc(func(params authop.DeletePolicyParams, user *models.User) middleware.Responder {
		ctx, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.DeletePolicyAction,
				Resource: permissions.PolicyArn(params.PolicyID),
			},
		})
		if err != nil {
			return authop.NewDeletePolicyUnauthorized().
				WithPayload(responseErrorFrom(err))
		}

		c.LogAction(ctx, "delete_policy")
		err = c.Auth.DeletePolicy(ctx, params.PolicyID)
		if errors.Is(err, auth.ErrNotFound) {
			return authop.NewDeletePolicyNotFound().
				WithPayload(responseError("policy not found"))
		}
		if err != nil {
			return authop.NewDeletePolicyDefault(http.StatusInternalServerError).
				WithPayload(responseErrorFrom(err))
		}
		return authop.NewDeletePolicyNoContent()
	})
}

func (c *Controller) ListGroupMembersHandler() authop.ListGroupMembersHandler {
	return authop.ListGroupMembersHandlerFunc(func(params authop.ListGroupMembersParams, user *models.User) middleware.Responder {
		ctx, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.ReadGroupAction,
				Resource: permissions.GroupArn(params.GroupID),
			},
		})
		if err != nil {
			return authop.NewListGroupMembersUnauthorized().
				WithPayload(responseErrorFrom(err))
		}

		c.LogAction(ctx, "list_group_users")
		users, paginator, err := c.Auth.ListGroupUsers(ctx, params.GroupID, &model.PaginationParams{
			After:  swag.StringValue(params.After),
			Amount: pageAmount(params.Amount),
		})
		if err != nil {
			return authop.NewListGroupMembersDefault(http.StatusInternalServerError).
				WithPayload(responseErrorFrom(err))
		}

		response := make([]*models.User, len(users))
		for i, u := range users {
			response[i] = &models.User{
				CreationDate: u.CreatedAt.Unix(),
				ID:           u.Username,
			}
		}

		return authop.NewListGroupMembersOK().
			WithPayload(&authop.ListGroupMembersOKBody{
				Pagination: createPaginator(paginator.NextPageToken, len(response)),
				Results:    response,
			})
	})
}

func (c *Controller) AddGroupMembershipHandler() authop.AddGroupMembershipHandler {
	return authop.AddGroupMembershipHandlerFunc(func(params authop.AddGroupMembershipParams, user *models.User) middleware.Responder {
		ctx, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.AddGroupMemberAction,
				Resource: permissions.GroupArn(params.GroupID),
			},
		})
		if err != nil {
			return authop.NewAddGroupMembershipUnauthorized().
				WithPayload(responseErrorFrom(err))
		}

		c.LogAction(ctx, "add_user_to_group")
		err = c.Auth.AddUserToGroup(ctx, params.UserID, params.GroupID)
		if err != nil {
			return authop.NewAddGroupMembershipDefault(http.StatusInternalServerError).
				WithPayload(responseErrorFrom(err))
		}

		return authop.NewAddGroupMembershipCreated()
	})
}

func (c *Controller) DeleteGroupMembershipHandler() authop.DeleteGroupMembershipHandler {
	return authop.DeleteGroupMembershipHandlerFunc(func(params authop.DeleteGroupMembershipParams, user *models.User) middleware.Responder {
		ctx, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.RemoveGroupMemberAction,
				Resource: permissions.GroupArn(params.GroupID),
			},
		})
		if err != nil {
			return authop.NewDeleteGroupMembershipUnauthorized().
				WithPayload(responseErrorFrom(err))
		}

		c.LogAction(ctx, "remove_user_from_group")
		err = c.Auth.RemoveUserFromGroup(ctx, params.UserID, params.GroupID)
		if err != nil {
			return authop.NewDeleteGroupMembershipDefault(http.StatusInternalServerError).
				WithPayload(responseErrorFrom(err))
		}

		return authop.NewDeleteGroupMembershipNoContent()
	})
}

func (c *Controller) ListUserCredentialsHandler() authop.ListUserCredentialsHandler {
	return authop.ListUserCredentialsHandlerFunc(func(params authop.ListUserCredentialsParams, user *models.User) middleware.Responder {
		ctx, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.ListCredentialsAction,
				Resource: permissions.UserArn(params.UserID),
			},
		})
		if err != nil {
			return authop.NewListUserCredentialsUnauthorized().
				WithPayload(responseErrorFrom(err))
		}

		c.LogAction(ctx, "list_user_credentials")
		credentials, paginator, err := c.Auth.ListUserCredentials(ctx, params.UserID, &model.PaginationParams{
			After:  swag.StringValue(params.After),
			Amount: pageAmount(params.Amount),
		})
		if err != nil {
			return authop.NewListUserCredentialsDefault(http.StatusInternalServerError).
				WithPayload(responseErrorFrom(err))
		}

		response := make([]*models.Credentials, len(credentials))
		for i, c := range credentials {
			response[i] = &models.Credentials{
				AccessKeyID:  c.AccessKeyID,
				CreationDate: c.IssuedDate.Unix(),
			}
		}

		return authop.NewListUserCredentialsOK().
			WithPayload(&authop.ListUserCredentialsOKBody{
				Pagination: createPaginator(paginator.NextPageToken, len(response)),
				Results:    response,
			})
	})
}

func (c *Controller) CreateCredentialsHandler() authop.CreateCredentialsHandler {
	return authop.CreateCredentialsHandlerFunc(func(params authop.CreateCredentialsParams, user *models.User) middleware.Responder {
		ctx, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.CreateCredentialsAction,
				Resource: permissions.UserArn(params.UserID),
			},
		})
		if err != nil {
			return authop.NewCreateCredentialsUnauthorized().
				WithPayload(responseErrorFrom(err))
		}

		c.LogAction(ctx, "create_credentials")
		credentials, err := c.Auth.CreateCredentials(ctx, params.UserID)
		if err != nil {
			return authop.NewCreateCredentialsDefault(http.StatusInternalServerError).
				WithPayload(responseErrorFrom(err))
		}

		return authop.NewCreateCredentialsCreated().
			WithPayload(&models.CredentialsWithSecret{
				AccessKeyID:     credentials.AccessKeyID,
				AccessSecretKey: credentials.AccessSecretKey,
				CreationDate:    credentials.IssuedDate.Unix(),
			})
	})
}

func (c *Controller) DeleteCredentialsHandler() authop.DeleteCredentialsHandler {
	return authop.DeleteCredentialsHandlerFunc(func(params authop.DeleteCredentialsParams, user *models.User) middleware.Responder {
		ctx, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.DeleteCredentialsAction,
				Resource: permissions.UserArn(params.UserID),
			},
		})
		if err != nil {
			return authop.NewDeleteCredentialsUnauthorized().
				WithPayload(responseErrorFrom(err))
		}

		c.LogAction(ctx, "delete_credentials")
		err = c.Auth.DeleteCredentials(ctx, params.UserID, params.AccessKeyID)
		if errors.Is(err, auth.ErrNotFound) {
			return authop.NewDeleteCredentialsNotFound().
				WithPayload(responseError("credentials not found"))
		}
		if err != nil {
			return authop.NewDeleteCredentialsDefault(http.StatusInternalServerError).
				WithPayload(responseErrorFrom(err))
		}

		return authop.NewDeleteCredentialsNoContent()
	})
}

func (c *Controller) GetCredentialsHandler() authop.GetCredentialsHandler {
	return authop.GetCredentialsHandlerFunc(func(params authop.GetCredentialsParams, user *models.User) middleware.Responder {
		ctx, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.ReadCredentialsAction,
				Resource: permissions.UserArn(params.UserID),
			},
		})
		if err != nil {
			return authop.NewGetCredentialsUnauthorized().
				WithPayload(responseErrorFrom(err))
		}
		c.LogAction(ctx, "get_credentials_for_user")
		credentials, err := c.Auth.GetCredentialsForUser(ctx, params.UserID, params.AccessKeyID)
		if errors.Is(err, auth.ErrNotFound) {
			return authop.NewGetCredentialsNotFound().
				WithPayload(responseError("credentials not found"))
		}
		if err != nil {
			return authop.NewGetCredentialsDefault(http.StatusInternalServerError).
				WithPayload(responseErrorFrom(err))
		}

		return authop.NewGetCredentialsOK().
			WithPayload(&models.Credentials{
				AccessKeyID:  credentials.AccessKeyID,
				CreationDate: credentials.IssuedDate.Unix(),
			})
	})
}

func (c *Controller) ListUserGroupsHandler() authop.ListUserGroupsHandler {
	return authop.ListUserGroupsHandlerFunc(func(params authop.ListUserGroupsParams, user *models.User) middleware.Responder {
		ctx, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.ReadUserAction,
				Resource: permissions.UserArn(params.UserID),
			},
		})
		if err != nil {
			return authop.NewListUserGroupsUnauthorized().
				WithPayload(responseErrorFrom(err))
		}

		c.LogAction(ctx, "list_user_groups")
		groups, paginator, err := c.Auth.ListUserGroups(ctx, params.UserID, &model.PaginationParams{
			After:  swag.StringValue(params.After),
			Amount: pageAmount(params.Amount),
		})
		if err != nil {
			return authop.NewListUserGroupsDefault(http.StatusInternalServerError).
				WithPayload(responseErrorFrom(err))
		}

		response := make([]*models.Group, len(groups))
		for i, g := range groups {
			response[i] = &models.Group{
				CreationDate: g.CreatedAt.Unix(),
				ID:           g.DisplayName,
			}
		}

		return authop.NewListUserGroupsOK().
			WithPayload(&authop.ListUserGroupsOKBody{
				Pagination: createPaginator(paginator.NextPageToken, len(response)),
				Results:    response,
			})
	})
}

func (c *Controller) ListUserPoliciesHandler() authop.ListUserPoliciesHandler {
	return authop.ListUserPoliciesHandlerFunc(func(params authop.ListUserPoliciesParams, user *models.User) middleware.Responder {
		ctx, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.ReadUserAction,
				Resource: permissions.UserArn(params.UserID),
			},
		})
		if err != nil {
			return authop.NewListUserPoliciesUnauthorized().
				WithPayload(responseErrorFrom(err))
		}

		c.LogAction(ctx, "list_user_policies")
		var policies []*model.Policy
		var paginator *model.Paginator
		if swag.BoolValue(params.Effective) {
			policies, paginator, err = c.Auth.ListEffectivePolicies(ctx, params.UserID, &model.PaginationParams{
				After:  swag.StringValue(params.After),
				Amount: pageAmount(params.Amount),
			})
		} else {
			policies, paginator, err = c.Auth.ListUserPolicies(ctx, params.UserID, &model.PaginationParams{
				After:  swag.StringValue(params.After),
				Amount: pageAmount(params.Amount),
			})
		}

		if err != nil {
			return authop.NewListUserPoliciesDefault(http.StatusInternalServerError).
				WithPayload(responseErrorFrom(err))
		}

		response := make([]*models.Policy, len(policies))
		for i, p := range policies {
			response[i] = serializePolicy(p)
		}

		return authop.NewListUserPoliciesOK().
			WithPayload(&authop.ListUserPoliciesOKBody{
				Pagination: createPaginator(paginator.NextPageToken, len(response)),
				Results:    response,
			})
	})
}

func (c *Controller) AttachPolicyToUserHandler() authop.AttachPolicyToUserHandler {
	return authop.AttachPolicyToUserHandlerFunc(func(params authop.AttachPolicyToUserParams, user *models.User) middleware.Responder {
		ctx, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.AttachPolicyAction,
				Resource: permissions.UserArn(params.UserID),
			},
		})
		if err != nil {
			return authop.NewAttachPolicyToUserUnauthorized().
				WithPayload(responseErrorFrom(err))
		}

		c.LogAction(ctx, "attach_policy_to_user")
		err = c.Auth.AttachPolicyToUser(ctx, params.PolicyID, params.UserID)
		if err != nil {
			return authop.NewAttachPolicyToUserDefault(http.StatusInternalServerError).
				WithPayload(responseErrorFrom(err))
		}

		return authop.NewAttachPolicyToUserCreated()
	})
}

func (c *Controller) DetachPolicyFromUserHandler() authop.DetachPolicyFromUserHandler {
	return authop.DetachPolicyFromUserHandlerFunc(func(params authop.DetachPolicyFromUserParams, user *models.User) middleware.Responder {
		ctx, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.DetachPolicyAction,
				Resource: permissions.UserArn(params.UserID),
			},
		})
		if err != nil {
			return authop.NewDetachPolicyFromUserUnauthorized().
				WithPayload(responseErrorFrom(err))
		}

		c.LogAction(ctx, "detach_policy_from_user")
		err = c.Auth.DetachPolicyFromUser(ctx, params.PolicyID, params.UserID)
		if err != nil {
			return authop.NewDetachPolicyFromUserDefault(http.StatusInternalServerError).
				WithPayload(responseErrorFrom(err))
		}

		return authop.NewDetachPolicyFromUserNoContent()
	})
}

func (c *Controller) ListGroupPoliciesHandler() authop.ListGroupPoliciesHandler {
	return authop.ListGroupPoliciesHandlerFunc(func(params authop.ListGroupPoliciesParams, user *models.User) middleware.Responder {
		ctx, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.ReadGroupAction,
				Resource: permissions.GroupArn(params.GroupID),
			},
		})
		if err != nil {
			return authop.NewListGroupPoliciesUnauthorized().
				WithPayload(responseErrorFrom(err))
		}

		c.LogAction(ctx, "list_user_policies")
		policies, paginator, err := c.Auth.ListGroupPolicies(ctx, params.GroupID, &model.PaginationParams{
			After:  swag.StringValue(params.After),
			Amount: pageAmount(params.Amount),
		})
		if err != nil {
			return authop.NewListGroupPoliciesDefault(http.StatusInternalServerError).
				WithPayload(responseErrorFrom(err))
		}

		response := make([]*models.Policy, len(policies))
		for i, p := range policies {
			response[i] = serializePolicy(p)
		}

		return authop.NewListGroupPoliciesOK().
			WithPayload(&authop.ListGroupPoliciesOKBody{
				Pagination: createPaginator(paginator.NextPageToken, len(response)),
				Results:    response,
			})
	})
}

func (c *Controller) AttachPolicyToGroupHandler() authop.AttachPolicyToGroupHandler {
	return authop.AttachPolicyToGroupHandlerFunc(func(params authop.AttachPolicyToGroupParams, user *models.User) middleware.Responder {
		ctx, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.AttachPolicyAction,
				Resource: permissions.GroupArn(params.GroupID),
			},
		})
		if err != nil {
			return authop.NewAttachPolicyToGroupUnauthorized().
				WithPayload(responseErrorFrom(err))
		}

		c.LogAction(ctx, "attach_policy_to_group")
		err = c.Auth.AttachPolicyToGroup(ctx, params.PolicyID, params.GroupID)
		if err != nil {
			return authop.NewAttachPolicyToGroupDefault(http.StatusInternalServerError).
				WithPayload(responseErrorFrom(err))
		}

		return authop.NewAttachPolicyToGroupCreated()
	})
}

func (c *Controller) DetachPolicyFromGroupHandler() authop.DetachPolicyFromGroupHandler {
	return authop.DetachPolicyFromGroupHandlerFunc(func(params authop.DetachPolicyFromGroupParams, user *models.User) middleware.Responder {
		ctx, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.DetachPolicyAction,
				Resource: permissions.GroupArn(params.GroupID),
			},
		})
		if err != nil {
			return authop.NewDetachPolicyFromGroupUnauthorized().
				WithPayload(responseErrorFrom(err))
		}

		c.LogAction(ctx, "detach_policy_from_group")
		err = c.Auth.DetachPolicyFromGroup(ctx, params.PolicyID, params.GroupID)
		if err != nil {
			return authop.NewDetachPolicyFromGroupDefault(http.StatusInternalServerError).
				WithPayload(responseErrorFrom(err))
		}

		return authop.NewDetachPolicyFromGroupNoContent()
	})
}

func (c *Controller) RefsRestoreHandler() refs.RestoreHandler {
	return refs.RestoreHandlerFunc(func(params refs.RestoreParams, user *models.User) middleware.Responder {
		ctx, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.CreateTagAction,
				Resource: permissions.RepoArn(params.Repository),
			},
			{
				Action:   permissions.CreateBranchAction,
				Resource: permissions.RepoArn(params.Repository),
			},
			{
				Action:   permissions.CreateCommitAction,
				Resource: permissions.RepoArn(params.Repository),
			},
		})
		if err != nil {
			return refs.NewRestoreUnauthorized().
				WithPayload(responseErrorFrom(err))
		}
		c.LogAction(ctx, "restore_repository_refs")

		repo, err := c.Catalog.GetRepository(ctx, params.Repository)
		if errors.Is(err, catalog.ErrRepositoryNotFound) {
			return refs.NewRestoreNotFound().
				WithPayload(responseErrorFrom(err))
		} else if err != nil {
			return refs.NewRestoreDefault(http.StatusInternalServerError).
				WithPayload(responseErrorFrom(err))
		}

		// ensure no refs currently found
		_, _, err = c.Catalog.ListCommits(ctx, repo.Name, repo.DefaultBranch, "", 1)
		if !errors.Is(err, graveler.ErrNotFound) {
			return refs.NewRestoreBadRequest().
				WithPayload(responseError("can only restore into a bare repository"))
		}

		// load commits
		err = c.Catalog.LoadCommits(ctx, repo.Name, params.Manifest.CommitsMetaRangeID)
		if err != nil {
			return refs.NewRestoreDefault(http.StatusInternalServerError).
				WithPayload(responseErrorFrom(err))
		}

		err = c.Catalog.LoadBranches(ctx, repo.Name, params.Manifest.BranchesMetaRangeID)
		if err != nil {
			return refs.NewRestoreDefault(http.StatusInternalServerError).
				WithPayload(responseErrorFrom(err))
		}

		err = c.Catalog.LoadTags(ctx, repo.Name, params.Manifest.TagsMetaRangeID)
		if err != nil {
			return refs.NewRestoreDefault(http.StatusInternalServerError).
				WithPayload(responseErrorFrom(err))
		}

		// DONE
		return refs.NewRestoreOK()
	})
}

func (c *Controller) RefsDumpHandler() refs.DumpHandler {
	return refs.DumpHandlerFunc(func(params refs.DumpParams, user *models.User) middleware.Responder {
		ctx, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.ListTagsAction,
				Resource: permissions.RepoArn(params.Repository),
			},
			{
				Action:   permissions.ListBranchesAction,
				Resource: permissions.RepoArn(params.Repository),
			},
			{
				Action:   permissions.ListCommitsAction,
				Resource: permissions.RepoArn(params.Repository),
			},
		})
		if err != nil {
			return refs.NewDumpUnauthorized().
				WithPayload(responseErrorFrom(err))
		}
		c.LogAction(ctx, "dump_repository_refs")

		repo, err := c.Catalog.GetRepository(ctx, params.Repository)
		if errors.Is(err, catalog.ErrRepositoryNotFound) {
			return refs.NewDumpNotFound().
				WithPayload(responseErrorFrom(err))
		} else if err != nil {
			return refs.NewDumpDefault(http.StatusInternalServerError).
				WithPayload(responseErrorFrom(err))
		}

		// dump all types:
		tagsID, err := c.Catalog.DumpTags(ctx, params.Repository)
		if err != nil {
			return refs.NewDumpDefault(http.StatusInternalServerError).
				WithPayload(responseErrorFrom(err))
		}
		branchesID, err := c.Catalog.DumpBranches(ctx, params.Repository)
		if err != nil {
			return refs.NewDumpDefault(http.StatusInternalServerError).
				WithPayload(responseErrorFrom(err))
		}
		commitsID, err := c.Catalog.DumpCommits(ctx, params.Repository)
		if err != nil {
			return refs.NewDumpDefault(http.StatusInternalServerError).
				WithPayload(responseErrorFrom(err))
		}
		manifestData := &models.RefsDump{
			BranchesMetaRangeID: branchesID,
			CommitsMetaRangeID:  commitsID,
			TagsMetaRangeID:     tagsID,
		}

		// write this to the block store
		manifestBytes, err := json.MarshalIndent(manifestData, "", "  ")
		if err != nil {
			return refs.NewDumpDefault(http.StatusInternalServerError).
				WithPayload(responseErrorFrom(err))
		}
		err = c.BlockAdapter.Put(ctx, block.ObjectPointer{
			StorageNamespace: repo.StorageNamespace,
			Identifier:       "_lakefs/refs_manifest.json",
		}, int64(len(manifestBytes)), bytes.NewReader(manifestBytes), block.PutOpts{})
		if err != nil {
			return refs.NewDumpDefault(http.StatusInternalServerError).
				WithPayload(responseErrorFrom(err))
		}

		return refs.NewDumpCreated().WithPayload(manifestData)
	})
}

func (c *Controller) ActionsGetRunHandler() actionsop.GetRunHandler {
	return actionsop.GetRunHandlerFunc(func(params actionsop.GetRunParams, user *models.User) middleware.Responder {
		ctx, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.ReadActionsAction,
				Resource: permissions.RepoArn(params.Repository),
			},
		})
		if err != nil {
			return actionsop.NewGetRunUnauthorized().
				WithPayload(responseErrorFrom(err))
		}

		_, err = c.Catalog.GetRepository(ctx, params.Repository)
		if errors.Is(err, catalog.ErrRepositoryNotFound) {
			return actionsop.NewListRunsNotFound().
				WithPayload(responseErrorFrom(catalog.ErrRepositoryNotFound))
		}
		if err != nil {
			return actionsop.NewListRunsDefault(http.StatusInternalServerError).
				WithPayload(responseErrorFrom(err))
		}

		runResult, err := c.Actions.GetRunResult(ctx, params.Repository, params.RunID)
		if err != nil {
			if errors.Is(err, actions.ErrNotFound) {
				return actionsop.NewGetRunNotFound().
					WithPayload(responseErrorFrom(err))
			}

			return actionsop.NewGetRunDefault(http.StatusInternalServerError).
				WithPayload(responseErrorFrom(err))
		}

		status := models.ActionRunStatusCompleted
		if !runResult.Passed {
			status = models.ActionRunStatusFailed
		}
		res := &models.ActionRun{
			RunID:     swag.String(runResult.RunID),
			EventType: runResult.EventType,
			StartTime: strfmt.DateTime(runResult.StartTime),
			EndTime:   strfmt.DateTime(runResult.EndTime),
			Status:    status,
			Branch:    swag.String(runResult.BranchID),
			CommitID:  swag.String(runResult.CommitID),
		}
		return actionsop.NewGetRunOK().WithPayload(res)
	})
}

func (c *Controller) ActionsGetRunHookOutputHandler() actionsop.GetRunHookOutputHandler {
	return actionsop.GetRunHookOutputHandlerFunc(func(params actionsop.GetRunHookOutputParams, user *models.User) middleware.Responder {
		ctx, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.ReadActionsAction,
				Resource: permissions.RepoArn(params.Repository),
			},
		})
		if err != nil {
			return actionsop.NewGetRunHookOutputUnauthorized().
				WithPayload(responseErrorFrom(err))
		}

		handleErr := func(err, target error) middleware.Responder {
			if errors.Is(err, target) {
				return actionsop.NewGetRunHookOutputNotFound().
					WithPayload(responseErrorFrom(err))
			}
			return actionsop.NewGetRunHookOutputDefault(http.StatusInternalServerError).
				WithPayload(responseErrorFrom(err))
		}

		repo, err := c.Catalog.GetRepository(ctx, params.Repository)
		if err != nil {
			return handleErr(err, catalog.ErrRepositoryNotFound)
		}

		taskResult, err := c.Actions.GetTaskResult(ctx, repo.Name, params.RunID, params.HookRunID)
		if err != nil {
			return handleErr(err, actions.ErrNotFound)
		}

		logPath := taskResult.LogPath()
		reader, err := c.BlockAdapter.Get(ctx, block.ObjectPointer{
			StorageNamespace: repo.StorageNamespace,
			Identifier:       logPath,
		}, -1)

		if err != nil {
			return actionsop.NewGetRunHookOutputDefault(http.StatusInternalServerError).
				WithPayload(responseErrorFrom(err))
		}
		return actionsop.NewGetRunHookOutputOK().WithPayload(reader)
	})
}

func (c *Controller) ActionsListRunHooksHandler() actionsop.ListRunHooksHandler {
	return actionsop.ListRunHooksHandlerFunc(func(params actionsop.ListRunHooksParams, user *models.User) middleware.Responder {
		ctx, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.ReadActionsAction,
				Resource: permissions.RepoArn(params.Repository),
			},
		})
		if err != nil {
			return actionsop.NewListRunHooksUnauthorized().
				WithPayload(responseErrorFrom(err))
		}

		handleErr := func(err, target error) middleware.Responder {
			if errors.Is(err, target) {
				return actionsop.NewListRunHooksNotFound().
					WithPayload(responseErrorFrom(err))
			}
			return actionsop.NewListRunHooksDefault(http.StatusInternalServerError).
				WithPayload(responseErrorFrom(err))
		}

		repo, err := c.Catalog.GetRepository(ctx, params.Repository)
		if err != nil {
			return handleErr(err, catalog.ErrRepositoryNotFound)
		}

		after, amount := getPaginationParams(params.After, params.Amount)
		tasksIter, err := c.Actions.ListRunTaskResults(ctx, repo.Name, params.RunID, after)
		if err != nil {
			return handleErr(err, actions.ErrNotFound)
		}
		defer tasksIter.Close()

		payload := &actionsop.ListRunHooksOKBody{
			Results: make([]*models.HookRun, 0),
			Pagination: &models.Pagination{
				HasMore: swag.Bool(false),
			},
		}
		var nextToken string
		for len(payload.Results) < amount && tasksIter.Next() {
			val := tasksIter.Value()
			hookRun := &models.HookRun{
				HookRunID: swag.String(val.HookRunID),
				Action:    val.ActionName,
				HookID:    val.HookID,
				StartTime: strfmt.DateTime(val.StartTime),
				EndTime:   strfmt.DateTime(val.EndTime),
			}
			if val.Passed {
				hookRun.Status = models.HookRunStatusCompleted
			} else {
				hookRun.Status = models.HookRunStatusFailed
			}
			payload.Results = append(payload.Results, hookRun)
			nextToken = val.HookRunID
		}
		if tasksIter.Next() {
			payload.Pagination.HasMore = swag.Bool(true)
			payload.Pagination.NextOffset = nextToken
		}
		if err := tasksIter.Err(); err != nil {
			return actionsop.NewListRunHooksDefault(http.StatusInternalServerError).
				WithPayload(responseErrorFrom(err))
		}
		payload.Pagination.Results = swag.Int64(int64(len(payload.Results)))
		return actionsop.NewListRunHooksOK().WithPayload(payload)
	})
}

func (c *Controller) ActionsListRunsHandler() actionsop.ListRunsHandler {
	return actionsop.ListRunsHandlerFunc(func(params actionsop.ListRunsParams, user *models.User) middleware.Responder {
		ctx, err := c.setupRequest(user, params.HTTPRequest, []permissions.Permission{
			{
				Action:   permissions.ReadActionsAction,
				Resource: permissions.RepoArn(params.Repository),
			},
		})
		if err != nil {
			return actionsop.NewListRunsUnauthorized().
				WithPayload(responseErrorFrom(err))
		}

		_, err = c.Catalog.GetRepository(ctx, params.Repository)
		if errors.Is(err, catalog.ErrRepositoryNotFound) {
			return actionsop.NewListRunsNotFound().
				WithPayload(responseErrorFrom(catalog.ErrRepositoryNotFound))
		}
		if err != nil {
			return actionsop.NewListRunsDefault(http.StatusInternalServerError).
				WithPayload(responseErrorFrom(err))
		}
		if params.Branch != nil {
			exists, err := c.Catalog.BranchExists(ctx, params.Repository, swag.StringValue(params.Branch))
			if err != nil {
				return actionsop.NewListRunsDefault(http.StatusInternalServerError).
					WithPayload(responseErrorFrom(err))
			}
			if !exists {
				return actionsop.NewListRunsNotFound().
					WithPayload(responseErrorFrom(catalog.ErrBranchNotFound))
			}
		}

		after, amount := getPaginationParams(params.After, params.Amount)
		runsIter, err := c.Actions.ListRunResults(ctx, params.Repository, swag.StringValue(params.Branch), swag.StringValue(params.Commit), after)
		if err != nil {
			if errors.Is(err, actions.ErrNotFound) {
				return actionsop.NewListRunsNotFound().
					WithPayload(responseErrorFrom(err))
			}
			return actionsop.NewListRunsDefault(http.StatusInternalServerError).
				WithPayload(responseErrorFrom(err))
		}

		payload := &actionsop.ListRunsOKBody{
			Results: make([]*models.ActionRun, 0),
			Pagination: &models.Pagination{
				HasMore: swag.Bool(false),
			},
		}
		var nextToken string
		for len(payload.Results) < amount && runsIter.Next() {
			val := runsIter.Value()
			payload.Results = append(payload.Results, convertRun(val))
			nextToken = val.RunID
		}
		payload.Pagination.Results = swag.Int64(int64(len(payload.Results)))
		if runsIter.Next() {
			payload.Pagination.HasMore = swag.Bool(true)
			payload.Pagination.NextOffset = nextToken
		}
		if err := runsIter.Err(); err != nil {
			return actionsop.NewListRunsDefault(http.StatusInternalServerError).
				WithPayload(responseErrorFrom(err))
		}
		return actionsop.NewListRunsOK().WithPayload(payload)
	})
}

func convertRun(val *actions.RunResult) *models.ActionRun {
	runResult := &models.ActionRun{
		Branch:    swag.String(val.BranchID),
		CommitID:  swag.String(val.CommitID),
		RunID:     swag.String(val.RunID),
		StartTime: strfmt.DateTime(val.StartTime),
		EndTime:   strfmt.DateTime(val.EndTime),
		EventType: val.EventType,
	}
	if val.Passed {
		runResult.Status = models.HookRunStatusCompleted
	} else {
		runResult.Status = models.HookRunStatusFailed
	}
	return runResult
}
