package api

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"time"

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

	api.BranchesDiffBranchesHandler = a.BranchesDiffBranchesHandler()
	api.BranchesDiffBranchHandler = a.BranchesDiffBranchHandler()

	api.ObjectsStatObjectHandler = a.ObjectsStatObjectHandler()
	api.ObjectsListObjectsHandler = a.ObjectsListObjectsHandler()
	api.ObjectsGetObjectHandler = a.ObjectsGetObjectHandler()
	api.ObjectsUploadObjectHandler = a.ObjectsUploadObjectHandler()
	api.ObjectsDeleteObjectHandler = a.ObjectsDeleteObjectHandler()
}

func (a *Handler) authorize(user *models.User, action permissions.Action) error {
	return authorize(a.auth, user, action)
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
		err := a.authorize(user, permissions.GetRepo(params.RepositoryID))
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
		err := a.authorize(user, permissions.GetCommit(params.RepositoryID))
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
		commit, err := a.meta.Commit(params.RepositoryID, params.BranchID, *params.Commit.Message, user.ID, params.Commit.Metadata)
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

		// read branch
		branch, err := a.meta.GetBranch(params.RepositoryID, params.BranchID)
		if err != nil {
			if xerrors.Is(err, db.ErrNotFound) {
				return commits.NewGetBranchCommitLogNotFound().WithPayload(responseErrorFrom(err))
			}
			return commits.NewGetBranchCommitLogDefault(http.StatusInternalServerError).WithPayload(responseErrorFrom(err))
		}

		// get commit log
		commitLog, err := a.meta.GetCommitLog(params.RepositoryID, branch.GetCommit())
		if err != nil {
			return commits.NewGetBranchCommitLogDefault(http.StatusInternalServerError).WithPayload(responseErrorFrom(err))
		}

		serializedCommits := make([]*models.Commit, len(commitLog))
		for i, commit := range commitLog {
			serializedCommits[i] = &models.Commit{
				Committer:    commit.GetCommitter(),
				CreationDate: commit.GetTimestamp(),
				ID:           commit.GetAddress(),
				Message:      commit.GetMessage(),
				Metadata:     commit.GetMetadata(),
				Parents:      commit.GetParents(),
			}
		}

		return commits.NewGetBranchCommitLogOK().WithPayload(&commits.GetBranchCommitLogOKBody{
			Results: serializedCommits,
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
		err := a.authorize(user, permissions.CreateRepo())
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
				WithPayload(responseError(fmt.Sprintf("error creating repository: %s", err)))
		}

		repo, err := a.meta.GetRepo(swag.StringValue(params.Repository.ID))
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
		err := a.authorize(user, permissions.ListBranches(params.RepositoryID))
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

		res, hasMore, err := a.meta.ListBranchesByPrefix(params.RepositoryID, "", int(amount), after)
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
		err := a.authorize(user, permissions.GetBranch(params.RepositoryID))
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
		err := a.authorize(user, permissions.CreateBranch(params.RepositoryID))
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
		err := a.authorize(user, permissions.DeleteBranch(params.RepositoryID))
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

func (a *Handler) BranchesDiffBranchHandler() branches.DiffBranchHandler {
	return branches.DiffBranchHandlerFunc(func(params branches.DiffBranchParams, user *models.User) middleware.Responder {
		err := a.authorize(user, permissions.DiffBranches(params.RepositoryID))
		if err != nil {
			return branches.NewDiffBranchesUnauthorized().WithPayload(responseErrorFrom(err))
		}

		diff, err := a.meta.DiffWorkspace(params.RepositoryID, params.BranchID)
		if err != nil {
			return branches.NewDiffBranchDefault(http.StatusInternalServerError).
				WithPayload(responseError("could not diff branches"))
		}

		results := make([]*models.Diff, len(diff))
		for i, d := range diff {
			results[i] = serializeDiff(d)
		}

		return branches.NewDiffBranchOK().WithPayload(&branches.DiffBranchOKBody{Results: results})
	})
}

func (a *Handler) BranchesDiffBranchesHandler() branches.DiffBranchesHandler {
	return branches.DiffBranchesHandlerFunc(func(params branches.DiffBranchesParams, user *models.User) middleware.Responder {
		err := a.authorize(user, permissions.DiffBranches(params.RepositoryID))
		if err != nil {
			return branches.NewDiffBranchesUnauthorized().WithPayload(responseErrorFrom(err))
		}

		diff, err := a.meta.Diff(params.RepositoryID, params.BranchID, params.OtherBranchID)
		if err != nil {
			return branches.NewDiffBranchesDefault(http.StatusInternalServerError).
				WithPayload(responseError("could not diff branches"))
		}

		results := make([]*models.Diff, len(diff))
		for i, d := range diff {
			results[i] = serializeDiff(d)
		}

		return branches.NewDiffBranchesOK().WithPayload(&branches.DiffBranchesOKBody{Results: results})
	})
}

func (a *Handler) ObjectsStatObjectHandler() objects.StatObjectHandler {
	return objects.StatObjectHandlerFunc(func(params objects.StatObjectParams, user *models.User) middleware.Responder {
		err := a.authorize(user, permissions.GetObject(params.RepositoryID))
		if err != nil {
			return objects.NewStatObjectUnauthorized().WithPayload(responseErrorFrom(err))
		}

		// read metadata
		entry, err := a.meta.ReadEntry(params.RepositoryID, params.BranchID, params.Path)
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

		// read repo
		repo, err := a.meta.GetRepo(params.RepositoryID)
		if err != nil {
			if xerrors.Is(err, db.ErrNotFound) {
				return objects.NewGetObjectNotFound().WithPayload(responseError("resource not found"))
			} else {
				return objects.NewGetObjectDefault(http.StatusInternalServerError).WithPayload(responseErrorFrom(err))
			}
		}

		// read the FS entry
		entry, err := a.meta.ReadEntry(params.RepositoryID, params.BranchID, params.Path)
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

		// get object for its blocks
		obj, err := a.meta.ReadObject(params.RepositoryID, params.BranchID, params.Path)
		if err != nil {
			return objects.NewGetObjectDefault(http.StatusInternalServerError).WithPayload(responseErrorFrom(err))
		}

		// build a response as a multi-reader
		res.ContentLength = obj.GetSize()
		blocks := obj.GetBlocks()
		readers := make([]io.ReadCloser, len(blocks))
		for i, block := range blocks {
			reader, err := a.blockAdapter.Get(repo.GetBucketName(), block.GetAddress())
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

		res, hasMore, err := a.meta.ListObjectsByPrefix(params.RepositoryID, params.BranchID, swag.StringValue(params.Tree), after, int(amount), false)
		if err != nil {
			return objects.NewListObjectsDefault(http.StatusInternalServerError).
				WithPayload(responseError("received error while listing objects"))
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

		repo, err := a.meta.GetRepo(params.RepositoryID)
		if err != nil {
			if xerrors.Is(err, db.ErrNotFound) {
				return objects.NewUploadObjectNotFound().WithPayload(responseError("resource not found"))
			} else {
				return objects.NewUploadObjectDefault(http.StatusInternalServerError).WithPayload(responseErrorFrom(err))
			}
		}

		// read the content
		blob, err := upload.ReadBlob(repo.GetBucketName(), params.Content, a.blockAdapter, upload.ObjectBlockSize)
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
		err = a.meta.WriteFile(params.RepositoryID, params.BranchID, params.Path, entry, obj)
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

		err = a.meta.DeleteObject(params.RepositoryID, params.BranchID, params.Path)
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
		switch swag.StringValue(params.Revert.Type) {
		case models.RevertCreationTypeCOMMIT:
			err = a.meta.RevertCommit(params.RepositoryID, params.BranchID, params.Revert.Commit)

		case models.RevertCreationTypeTREE:
			err = a.meta.RevertPath(params.RepositoryID, params.BranchID, params.Revert.Path)

		case models.RevertCreationTypeRESET:
			err = a.meta.ResetBranch(params.RepositoryID, params.BranchID)

		case models.RevertCreationTypeOBJECT:
			err = a.meta.RevertObject(params.RepositoryID, params.BranchID, params.Revert.Path)
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
