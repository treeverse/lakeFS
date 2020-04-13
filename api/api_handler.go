package api

import (
	"bytes"
	"context"
	"fmt"
	"github.com/treeverse/lakefs/api/gen/restapi/operations/merge"
	"github.com/treeverse/lakefs/index/errors"
	"io"
	"net/http"
	"time"

	"github.com/treeverse/lakefs/api/gen/restapi/operations/refs"

	"github.com/treeverse/lakefs/api/gen/restapi/operations/authentication"

	"github.com/treeverse/lakefs/ident"
	pth "github.com/treeverse/lakefs/index/path"

	"github.com/treeverse/lakefs/upload"

	"github.com/treeverse/lakefs/httputil"

	"github.com/treeverse/lakefs/api/gen/restapi/operations/objects"

	"github.com/treeverse/lakefs/index/model"

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
	"github.com/treeverse/lakefs/index"
	"github.com/treeverse/lakefs/permissions"
	"golang.org/x/xerrors"
)

const (
	// Maximum amount of results returned for paginated queries to the API
	MaxResultsPerPage int64 = 1000
)

type HandlerContext struct {
	Index        index.Index
	Auth         auth.Service
	BlockAdapter block.Adapter
}

func (c *HandlerContext) WithContext(ctx context.Context) *HandlerContext {
	return &HandlerContext{
		Index:        c.Index.WithContext(ctx),
		Auth:         c.Auth, // TODO: pass context
		BlockAdapter: c.BlockAdapter.WithContext(ctx),
	}
}

type Handler struct {
	context *HandlerContext
}

func NewHandler(meta index.Index, auth auth.Service, blockAdapter block.Adapter) *Handler {
	return &Handler{
		context: &HandlerContext{
			Index:        meta,
			Auth:         auth,
			BlockAdapter: blockAdapter,
		},
	}
}

func (a *Handler) ForRequest(r *http.Request) *HandlerContext {
	return a.context.WithContext(r.Context())
}

// Configure attaches our API operations to a generated swagger API stub
// Adding new handlers requires also adding them here so that the generated server will use them
func (a *Handler) Configure(api *operations.LakefsAPI) {

	// Register operations here
	api.AuthenticationGetAuthenticationHandler = a.AuthenticationGetAuthenticationHandler()

	api.RepositoriesListRepositoriesHandler = a.ListRepositoriesHandler()
	api.RepositoriesGetRepositoryHandler = a.GetRepoHandler()
	api.RepositoriesCreateRepositoryHandler = a.CreateRepositoryHandler()
	api.RepositoriesDeleteRepositoryHandler = a.DeleteRepositoryHandler()

	api.BranchesListBranchesHandler = a.ListBranchesHandler()
	api.BranchesGetBranchHandler = a.GetBranchHandler()
	api.BranchesCreateBranchHandler = a.CreateBranchHandler()
	api.BranchesDeleteBranchHandler = a.DeleteBranchHandler()
	api.BranchesRevertBranchHandler = a.RevertBranchHandler()

	api.CommitsCommitHandler = a.CommitHandler()
	api.CommitsGetCommitHandler = a.GetCommitHandler()
	api.CommitsGetBranchCommitLogHandler = a.CommitsGetBranchCommitLogHandler()

	api.RefsDiffRefsHandler = a.RefsDiffRefsHandler()
	api.BranchesDiffBranchHandler = a.BranchesDiffBranchHandler()
	api.MergeMergeIntoBranchHandler = a.MergeMergeIntoBranchHandler()

	api.ObjectsStatObjectHandler = a.ObjectsStatObjectHandler()
	api.ObjectsListObjectsHandler = a.ObjectsListObjectsHandler()
	api.ObjectsGetObjectHandler = a.ObjectsGetObjectHandler()
	api.ObjectsUploadObjectHandler = a.ObjectsUploadObjectHandler()
	api.ObjectsDeleteObjectHandler = a.ObjectsDeleteObjectHandler()
}

func (a *Handler) authorize(user *models.User, action permissions.Action) error {
	return authorize(a.context.Auth, user, action)
}

func (a *Handler) AuthenticationGetAuthenticationHandler() authentication.GetAuthenticationHandler {
	return authentication.GetAuthenticationHandlerFunc(func(params authentication.GetAuthenticationParams, user *models.User) middleware.Responder {
		return authentication.NewGetAuthenticationOK().WithPayload(&authentication.GetAuthenticationOKBody{User: user})
	})
}

func (a *Handler) ListRepositoriesHandler() repositories.ListRepositoriesHandler {
	return repositories.ListRepositoriesHandlerFunc(func(params repositories.ListRepositoriesParams, user *models.User) middleware.Responder {
		err := a.authorize(user, permissions.ListRepos())
		if err != nil {
			return repositories.NewListRepositoriesUnauthorized().WithPayload(responseErrorFrom(err))
		}

		after, amount := getPaginationParams(params.After, params.Amount)

		repos, hasMore, err := a.ForRequest(params.HTTPRequest).Index.ListRepos(amount, after)
		if err != nil {
			return repositories.NewListRepositoriesDefault(http.StatusInternalServerError).
				WithPayload(responseError("error listing repositories: %s", err))
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

func (a *Handler) GetRepoHandler() repositories.GetRepositoryHandler {
	return repositories.GetRepositoryHandlerFunc(func(params repositories.GetRepositoryParams, user *models.User) middleware.Responder {
		err := a.authorize(user, permissions.GetRepo(params.RepositoryID))
		if err != nil {
			return repositories.NewGetRepositoryUnauthorized().WithPayload(responseErrorFrom(err))
		}

		repo, err := a.ForRequest(params.HTTPRequest).Index.GetRepo(params.RepositoryID)
		if err != nil && xerrors.Is(err, db.ErrNotFound) {
			return repositories.NewGetRepositoryNotFound().
				WithPayload(responseError("repository not found"))
		} else if err != nil {
			return repositories.NewGetRepositoryDefault(http.StatusInternalServerError).
				WithPayload(responseError("error fetching repository: %s", err))
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
		err := a.authorize(user, permissions.GetCommit(params.RepositoryID))
		if err != nil {
			return commits.NewGetCommitUnauthorized().WithPayload(responseErrorFrom(err))
		}
		commit, err := a.ForRequest(params.HTTPRequest).Index.GetCommit(params.RepositoryID, params.CommitID)

		if xerrors.Is(err, db.ErrNotFound) {
			return commits.NewGetCommitNotFound().WithPayload(responseError("commit not found"))
		}
		if err != nil {
			return commits.NewGetCommitDefault(http.StatusInternalServerError).WithPayload(responseErrorFrom(err))
		}
		return commits.NewGetCommitOK().WithPayload(&models.Commit{
			Committer:    commit.GetCommitter(),
			CreationDate: commit.GetTimestamp(),
			ID:           commit.GetAddress(),
			Message:      commit.GetMessage(),
			Metadata:     commit.GetMetadata(),
			Parents:      commit.GetParents(),
		})
	})
}

func (a *Handler) CommitHandler() commits.CommitHandler {
	return commits.CommitHandlerFunc(func(params commits.CommitParams, user *models.User) middleware.Responder {
		err := a.authorize(user, permissions.Commit(params.RepositoryID))
		if err != nil {
			return commits.NewCommitUnauthorized().WithPayload(responseErrorFrom(err))
		}
		commit, err := a.ForRequest(params.HTTPRequest).Index.Commit(params.RepositoryID, params.BranchID, *params.Commit.Message, user.ID, params.Commit.Metadata)
		if err != nil {
			return commits.NewCommitDefault(http.StatusInternalServerError).WithPayload(responseErrorFrom(err))
		}
		return commits.NewCommitCreated().WithPayload(&models.Commit{
			Committer:    commit.GetCommitter(),
			CreationDate: commit.GetTimestamp(),
			ID:           commit.GetAddress(),
			Message:      commit.GetMessage(),
			Metadata:     commit.GetMetadata(),
			Parents:      commit.GetParents(),
		})
	})
}

func (a *Handler) CommitsGetBranchCommitLogHandler() commits.GetBranchCommitLogHandler {
	return commits.GetBranchCommitLogHandlerFunc(func(params commits.GetBranchCommitLogParams, user *models.User) middleware.Responder {
		err := a.authorize(user, permissions.GetCommit(params.RepositoryID))
		if err != nil {
			return commits.NewGetBranchCommitLogUnauthorized().WithPayload(responseErrorFrom(err))
		}
		index := a.ForRequest(params.HTTPRequest).Index

		// read branch
		branch, err := index.GetBranch(params.RepositoryID, params.BranchID)
		if err != nil {
			if xerrors.Is(err, db.ErrNotFound) {
				return commits.NewGetBranchCommitLogNotFound().WithPayload(responseErrorFrom(err))
			}
			return commits.NewGetBranchCommitLogDefault(http.StatusInternalServerError).WithPayload(responseErrorFrom(err))
		}

		after, amount := getPaginationParams(params.After, params.Amount)
		// get commit log
		commitLog, hasMore, err := index.GetCommitLog(params.RepositoryID, branch.GetCommit(), amount, after)
		if err != nil {
			return commits.NewGetBranchCommitLogDefault(http.StatusInternalServerError).WithPayload(responseErrorFrom(err))
		}

		serializedCommits := make([]*models.Commit, len(commitLog))
		lastId := ""
		for i, commit := range commitLog {
			serializedCommits[i] = &models.Commit{
				Committer:    commit.GetCommitter(),
				CreationDate: commit.GetTimestamp(),
				ID:           commit.GetAddress(),
				Message:      commit.GetMessage(),
				Metadata:     commit.GetMetadata(),
				Parents:      commit.GetParents(),
			}
			lastId = commit.GetAddress()
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
		err := a.authorize(user, permissions.CreateRepo())
		if err != nil {
			return repositories.NewCreateRepositoryUnauthorized().WithPayload(responseErrorFrom(err))
		}
		ctx := a.ForRequest(params.HTTPRequest)

		err = testBucket(ctx.BlockAdapter, swag.StringValue(params.Repository.BucketName))
		if err != nil {
			return repositories.NewCreateRepositoryBadRequest().
				WithPayload(responseError("error creating repository: could not access bucket"))
		}
		err = ctx.Index.CreateRepo(swag.StringValue(params.Repository.ID), swag.StringValue(params.Repository.BucketName), params.Repository.DefaultBranch)
		if err != nil {
			return repositories.NewGetRepositoryDefault(http.StatusInternalServerError).
				WithPayload(responseError(fmt.Sprintf("error creating repository: %s", err)))
		}

		repo, err := ctx.Index.GetRepo(swag.StringValue(params.Repository.ID))
		if err != nil {
			return repositories.NewGetRepositoryDefault(http.StatusInternalServerError).
				WithPayload(responseError(fmt.Sprintf("error creating repository: %s", err)))
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
		err := a.authorize(user, permissions.DeleteRepo(params.RepositoryID))
		if err != nil {
			return repositories.NewDeleteRepositoryUnauthorized().WithPayload(responseErrorFrom(err))
		}
		index := a.ForRequest(params.HTTPRequest).Index
		err = index.DeleteRepo(params.RepositoryID)
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
		err := a.authorize(user, permissions.ListBranches(params.RepositoryID))
		if err != nil {
			return branches.NewListBranchesUnauthorized().WithPayload(responseErrorFrom(err))
		}
		index := a.ForRequest(params.HTTPRequest).Index

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
		err := a.authorize(user, permissions.GetBranch(params.RepositoryID))
		if err != nil {
			return branches.NewGetBranchUnauthorized().WithPayload(responseErrorFrom(err))
		}
		index := a.ForRequest(params.HTTPRequest).Index
		branch, err := index.GetBranch(params.RepositoryID, params.BranchID)
		if err != nil && xerrors.Is(err, db.ErrNotFound) {
			return branches.NewGetBranchNotFound().
				WithPayload(responseError("branch not found"))
		} else if err != nil {
			return branches.NewGetBranchDefault(http.StatusInternalServerError).
				WithPayload(responseError("error fetching branch: %s", err))
		}

		return branches.NewGetBranchOK().
			WithPayload(&models.Ref{
				CommitID: swag.String(branch.GetCommit()),
				ID:       swag.String(branch.GetName()),
			})
	})
}

func (a *Handler) CreateBranchHandler() branches.CreateBranchHandler {
	return branches.CreateBranchHandlerFunc(func(params branches.CreateBranchParams, user *models.User) middleware.Responder {
		err := a.authorize(user, permissions.CreateBranch(params.RepositoryID))
		if err != nil {
			return branches.NewCreateBranchUnauthorized().WithPayload(responseErrorFrom(err))
		}
		index := a.ForRequest(params.HTTPRequest).Index
		branch, err := index.CreateBranch(params.RepositoryID, swag.StringValue(params.Branch.ID), swag.StringValue(params.Branch.SourceRefID))
		if err != nil {
			return branches.NewCreateBranchDefault(http.StatusInternalServerError).WithPayload(responseErrorFrom(err))
		}

		return branches.NewCreateBranchCreated().WithPayload(&models.Ref{
			CommitID: swag.String(branch.GetCommit()),
			ID:       swag.String(branch.GetName()),
		})
	})
}

func (a *Handler) DeleteBranchHandler() branches.DeleteBranchHandler {
	return branches.DeleteBranchHandlerFunc(func(params branches.DeleteBranchParams, user *models.User) middleware.Responder {
		err := a.authorize(user, permissions.DeleteBranch(params.RepositoryID))
		if err != nil {
			return branches.NewDeleteBranchUnauthorized().WithPayload(responseErrorFrom(err))
		}
		index := a.ForRequest(params.HTTPRequest).Index
		err = index.DeleteBranch(params.RepositoryID, params.BranchID)
		if err != nil && xerrors.Is(err, db.ErrNotFound) {
			return branches.NewDeleteBranchNotFound().
				WithPayload(responseError("branch not found"))
		} else if err != nil {
			return branches.NewDeleteBranchDefault(http.StatusInternalServerError).
				WithPayload(responseError("error fetching branch: %s", err))
		}

		return branches.NewDeleteBranchNoContent()
	})
}

func (a *Handler) MergeMergeIntoBranchHandler() merge.MergeIntoBranchHandler {
	return merge.MergeIntoBranchHandlerFunc(func(params merge.MergeIntoBranchParams, user *models.User) middleware.Responder {
		err := a.authorize(user, permissions.MergeIntoBranch(params.RepositoryID))
		if err != nil {
			return merge.NewMergeIntoBranchUnauthorized().WithPayload(responseErrorFrom(err))
		}

		mergeOperations, err := a.context.Index.Merge(params.RepositoryID, params.RightRef, params.LeftRef, user.ID)
		mergeResult := make([]*models.MergeResult, len(mergeOperations))

		if err == nil || err == errors.ErrMergeConflict {
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
			pl := new(merge.MergeIntoBranchOKBody)
			pl.Results = mergeResult
			return merge.NewMergeIntoBranchOK().WithPayload(pl)
		case errors.ErrNoMergeBase:
			return merge.NewMergeIntoBranchDefault(http.StatusInternalServerError).WithPayload(responseError("branches have no common base"))
		case errors.ErrDestinationNotCommitted:
			return merge.NewMergeIntoBranchDefault(http.StatusInternalServerError).WithPayload(responseError("destination branch have not commited before "))
		case errors.ErrBranchNotFound:
			return merge.NewMergeIntoBranchDefault(http.StatusInternalServerError).WithPayload(responseError("a branch does not exist "))
		case errors.ErrMergeConflict:

			pl := new(merge.MergeIntoBranchConflictBody)
			pl.Results = mergeResult
			return merge.NewMergeIntoBranchConflict().WithPayload(pl)
		default:
			return merge.NewMergeIntoBranchDefault(http.StatusInternalServerError).WithPayload(responseError("internal error"))

		}

	})
}

func (a *Handler) BranchesDiffBranchHandler() branches.DiffBranchHandler {
	return branches.DiffBranchHandlerFunc(func(params branches.DiffBranchParams, user *models.User) middleware.Responder {
		err := a.authorize(user, permissions.DiffBranches(params.RepositoryID))
		if err != nil {
			return branches.NewDiffBranchUnauthorized().WithPayload(responseErrorFrom(err))
		}
		index := a.ForRequest(params.HTTPRequest).Index
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

func (a *Handler) RefsDiffRefsHandler() refs.DiffRefsHandler {
	return refs.DiffRefsHandlerFunc(func(params refs.DiffRefsParams, user *models.User) middleware.Responder {
		err := a.authorize(user, permissions.DiffBranches(params.RepositoryID))
		if err != nil {
			return refs.NewDiffRefsUnauthorized().WithPayload(responseErrorFrom(err))
		}
		index := a.ForRequest(params.HTTPRequest).Index
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

func (a *Handler) ObjectsStatObjectHandler() objects.StatObjectHandler {
	return objects.StatObjectHandlerFunc(func(params objects.StatObjectParams, user *models.User) middleware.Responder {
		err := a.authorize(user, permissions.GetObject(params.RepositoryID))
		if err != nil {
			return objects.NewStatObjectUnauthorized().WithPayload(responseErrorFrom(err))
		}
		index := a.ForRequest(params.HTTPRequest).Index

		// read metadata
		entry, err := index.ReadEntryObject(params.RepositoryID, params.Ref, params.Path)
		if err != nil {
			if xerrors.Is(err, db.ErrNotFound) {
				return objects.NewStatObjectNotFound().WithPayload(responseError("resource not found"))
			}
			return objects.NewStatObjectDefault(http.StatusInternalServerError).WithPayload(responseErrorFrom(err))
		}

		// serialize entry
		return objects.NewStatObjectOK().WithPayload(&models.ObjectStats{
			Checksum:  entry.GetChecksum(),
			Mtime:     entry.GetTimestamp(),
			Path:      params.Path,
			PathType:  models.ObjectStatsPathTypeOBJECT,
			SizeBytes: entry.GetSize(),
		})
	})
}

func (a *Handler) ObjectsGetObjectHandler() objects.GetObjectHandler {
	return objects.GetObjectHandlerFunc(func(params objects.GetObjectParams, user *models.User) middleware.Responder {
		err := a.authorize(user, permissions.GetObject(params.RepositoryID))
		if err != nil {
			return objects.NewGetObjectUnauthorized().WithPayload(responseErrorFrom(err))
		}
		ctx := a.ForRequest(params.HTTPRequest)
		index := ctx.Index

		// read repo
		repo, err := index.GetRepo(params.RepositoryID)
		if err != nil {
			if xerrors.Is(err, db.ErrNotFound) {
				return objects.NewGetObjectNotFound().WithPayload(responseError("resource not found"))
			} else {
				return objects.NewGetObjectDefault(http.StatusInternalServerError).WithPayload(responseErrorFrom(err))
			}
		}

		// read the FS entry
		entry, err := index.ReadEntryObject(params.RepositoryID, params.Ref, params.Path)
		if err != nil {
			if xerrors.Is(err, db.ErrNotFound) {
				return objects.NewGetObjectNotFound().WithPayload(responseError("resource not found"))
			} else {
				return objects.NewGetObjectDefault(http.StatusInternalServerError).WithPayload(responseErrorFrom(err))
			}
		}
		// setup response
		res := objects.NewGetObjectOK()
		res.ETag = httputil.ETag(entry.GetChecksum())
		res.LastModified = httputil.HeaderTimestamp(entry.GetTimestamp())
		res.ContentDisposition = fmt.Sprintf("filename=\"%s\"", entry.GetName())

		// get object for its blocks
		obj, err := index.ReadObject(params.RepositoryID, params.Ref, params.Path)
		if err != nil {
			return objects.NewGetObjectDefault(http.StatusInternalServerError).WithPayload(responseErrorFrom(err))
		}

		// build a response as a multi-reader
		res.ContentLength = obj.GetSize()
		blocks := obj.GetBlocks()
		readers := make([]io.ReadCloser, len(blocks))
		for i, block := range blocks {
			reader, err := ctx.BlockAdapter.Get(repo.GetBucketName(), block.GetAddress())
			if err != nil {
				return objects.NewGetObjectDefault(http.StatusInternalServerError).WithPayload(responseErrorFrom(err))
			}
			readers[i] = reader
		}

		// done
		res.Payload = NewMultiReadCloser(readers)
		return res
	})
}

func (a *Handler) ObjectsListObjectsHandler() objects.ListObjectsHandler {
	return objects.ListObjectsHandlerFunc(func(params objects.ListObjectsParams, user *models.User) middleware.Responder {
		err := a.authorize(user, permissions.GetObject(params.RepositoryID))
		if err != nil {
			return objects.NewListObjectsUnauthorized().WithPayload(responseErrorFrom(err))
		}
		index := a.ForRequest(params.HTTPRequest).Index

		after, amount := getPaginationParams(params.After, params.Amount)

		res, hasMore, err := index.ListObjectsByPrefix(params.RepositoryID, params.Ref, swag.StringValue(params.Tree), after, amount, false)
		if err != nil {
			if xerrors.Is(err, db.ErrNotFound) {
				return objects.NewListObjectsNotFound().WithPayload(responseError("could not find requested path"))
			}
			return objects.NewListObjectsDefault(http.StatusInternalServerError).
				WithPayload(responseError("error while listing objects: %s", err))
		}

		objList := make([]*models.ObjectStats, len(res))
		var lastId string
		for i, entry := range res {
			typ := models.ObjectStatsPathTypeTREE
			if entry.GetType() == model.Entry_OBJECT {
				typ = models.ObjectStatsPathTypeOBJECT
			}

			objList[i] = &models.ObjectStats{
				Checksum:  entry.GetChecksum(),
				Mtime:     entry.GetTimestamp(),
				Path:      entry.GetName(),
				PathType:  typ,
				SizeBytes: entry.GetSize(),
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

func (a *Handler) ObjectsUploadObjectHandler() objects.UploadObjectHandler {
	return objects.UploadObjectHandlerFunc(func(params objects.UploadObjectParams, user *models.User) middleware.Responder {
		err := a.authorize(user, permissions.WriteObject(params.RepositoryID))
		if err != nil {
			return objects.NewUploadObjectUnauthorized().WithPayload(responseErrorFrom(err))
		}
		ctx := a.ForRequest(params.HTTPRequest)
		index := ctx.Index

		repo, err := index.GetRepo(params.RepositoryID)
		if err != nil {
			if xerrors.Is(err, db.ErrNotFound) {
				return objects.NewUploadObjectNotFound().WithPayload(responseError("resource not found"))
			} else {
				return objects.NewUploadObjectDefault(http.StatusInternalServerError).WithPayload(responseErrorFrom(err))
			}
		}

		// read the content
		blob, err := upload.ReadBlob(repo.GetBucketName(), params.Content, ctx.BlockAdapter, upload.ObjectBlockSize)
		if err != nil {
			return objects.NewUploadObjectDefault(http.StatusInternalServerError).WithPayload(responseErrorFrom(err))
		}

		// write metadata
		writeTime := time.Now()
		obj := &model.Object{
			Blocks:   blob.Blocks,
			Checksum: blob.Checksum,
			Size:     blob.Size,
		}

		p := pth.New(params.Path)

		entry := &model.Entry{
			Name:      p.BaseName(),
			Address:   ident.Hash(obj),
			Type:      model.Entry_OBJECT,
			Timestamp: writeTime.Unix(),
			Size:      blob.Size,
			Checksum:  blob.Checksum,
		}
		err = index.WriteFile(params.RepositoryID, params.BranchID, params.Path, entry, obj)
		if err != nil {
			return objects.NewUploadObjectDefault(http.StatusInternalServerError).WithPayload(responseErrorFrom(err))
		}
		return objects.NewUploadObjectCreated().WithPayload(&models.ObjectStats{
			Checksum:  blob.Checksum,
			Mtime:     writeTime.Unix(),
			Path:      params.Path,
			PathType:  models.ObjectStatsPathTypeOBJECT,
			SizeBytes: blob.Size,
		})
	})
}

func (a *Handler) ObjectsDeleteObjectHandler() objects.DeleteObjectHandler {
	return objects.DeleteObjectHandlerFunc(func(params objects.DeleteObjectParams, user *models.User) middleware.Responder {
		err := a.authorize(user, permissions.DeleteObject(params.RepositoryID))
		if err != nil {
			return objects.NewDeleteObjectUnauthorized().WithPayload(responseErrorFrom(err))
		}
		index := a.ForRequest(params.HTTPRequest).Index

		err = index.DeleteObject(params.RepositoryID, params.BranchID, params.Path)
		if err != nil {
			if xerrors.Is(err, db.ErrNotFound) {
				return objects.NewDeleteObjectNotFound().WithPayload(responseError("resource not found"))
			} else {
				return objects.NewDeleteObjectDefault(http.StatusInternalServerError).WithPayload(responseErrorFrom(err))
			}
		}

		return objects.NewDeleteObjectNoContent()
	})
}
func (a *Handler) RevertBranchHandler() branches.RevertBranchHandler {
	return branches.RevertBranchHandlerFunc(func(params branches.RevertBranchParams, user *models.User) middleware.Responder {
		err := a.authorize(user, permissions.GetBranch(params.RepositoryID))
		if err != nil {
			return branches.NewRevertBranchUnauthorized().WithPayload(responseErrorFrom(err))
		}
		index := a.ForRequest(params.HTTPRequest).Index

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
		if err != nil {
			if xerrors.Is(err, db.ErrNotFound) {
				return branches.NewRevertBranchNotFound().
					WithPayload(responseError("branch not found"))
			} else {
				return branches.NewRevertBranchDefault(http.StatusInternalServerError).WithPayload(responseErrorFrom(err))
			}
		}

		return branches.NewRevertBranchNoContent()
	})
}
