package api

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/mail"
	"net/url"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/davecgh/go-spew/spew"
	"github.com/go-openapi/swag"
	"github.com/gorilla/sessions"
	"github.com/treeverse/lakefs/pkg/actions"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/acl"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/auth/setup"
	"github.com/treeverse/lakefs/pkg/authentication"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/cloud"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/graveler/ref"
	"github.com/treeverse/lakefs/pkg/httputil"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/permissions"
	"github.com/treeverse/lakefs/pkg/samplerepo"
	"github.com/treeverse/lakefs/pkg/stats"
	"github.com/treeverse/lakefs/pkg/upload"
	"github.com/treeverse/lakefs/pkg/validator"
	"github.com/treeverse/lakefs/pkg/version"
)

const (
	// DefaultMaxPerPage is the maximum number of results returned for paginated queries to the API
	DefaultMaxPerPage int = 1000
	// DefaultPerPage is the default number of results returned for paginated queries to the API
	DefaultPerPage int = 100

	lakeFSPrefix = "symlinks"

	actionStatusCompleted = "completed"
	actionStatusFailed    = "failed"
	actionStatusSkipped   = "skipped"

	entryTypeObject       = "object"
	entryTypeCommonPrefix = "common_prefix"

	DefaultMaxDeleteObjects = 1000

	// httpStatusClientClosedRequest used as internal status code when request context is cancelled
	httpStatusClientClosedRequest = 499
	// httpStatusClientClosedRequestText text used for client closed request status code
	httpStatusClientClosedRequestText = "Client closed request"
)

type actionsHandler interface {
	GetRunResult(ctx context.Context, repositoryID, runID string) (*actions.RunResult, error)
	GetTaskResult(ctx context.Context, repositoryID, runID, hookRunID string) (*actions.TaskResult, error)
	ListRunResults(ctx context.Context, repositoryID, branchID, commitID, after string) (actions.RunResultIterator, error)
	ListRunTaskResults(ctx context.Context, repositoryID, runID, after string) (actions.TaskResultIterator, error)
}

type Migrator interface {
	Migrate(ctx context.Context) error
}

type Controller struct {
	Config                *config.Config
	Catalog               *catalog.Catalog
	Authenticator         auth.Authenticator
	Auth                  auth.Service
	Authentication        authentication.Service
	BlockAdapter          block.Adapter
	MetadataManager       auth.MetadataManager
	Migrator              Migrator
	Collector             stats.Collector
	CloudMetadataProvider cloud.MetadataProvider
	Actions               actionsHandler
	AuditChecker          AuditChecker
	Logger                logging.Logger
	sessionStore          sessions.Store
	PathProvider          upload.PathProvider
	usageReporter         stats.UsageReporterOperations
}

var usageCounter = stats.NewUsageCounter()

func (c *Controller) DeleteUser(w http.ResponseWriter, r *http.Request, userID string) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.DeleteUserAction,
			Resource: permissions.UserArn(userID),
		},
	}) {
		return
	}

	ctx := r.Context()
	c.LogAction(ctx, "delete_user", r, "", "", "")
	err := c.Auth.DeleteUser(ctx, userID)
	if errors.Is(err, auth.ErrNotFound) {
		writeError(w, r, http.StatusNotFound, "user not found")
		return
	}
	if c.handleAPIError(ctx, w, r, err) {
		return
	}
	writeResponse(w, r, http.StatusNoContent, nil)
}

func (c *Controller) CreatePresignMultipartUpload(w http.ResponseWriter, r *http.Request, repository string, branch string, params apigen.CreatePresignMultipartUploadParams) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.WriteObjectAction,
			Resource: permissions.ObjectArn(repository, params.Path),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "create_presign_multipart_upload", r, repository, branch, "")

	// check if api is supported
	storageConfig := c.getStorageConfig()
	if !swag.BoolValue(storageConfig.PreSignMultipartUpload) {
		writeError(w, r, http.StatusNotImplemented, "presign multipart upload API is not supported")
		return
	}

	repo, err := c.Catalog.GetRepository(ctx, repository)
	if c.handleAPIError(ctx, w, r, err) {
		return
	}

	// check if the branch exists - it is still possible for a branch to be deleted later, but we don't want to
	// upload to start and fail at the end when the branch was not there in the first place
	branchExists, err := c.Catalog.BranchExists(ctx, repository, branch)
	if c.handleAPIError(ctx, w, r, err) {
		return
	}
	if !branchExists {
		writeError(w, r, http.StatusNotFound, fmt.Sprintf("branch '%s' not found", branch))
		return
	}

	// check if the path not empty
	if params.Path == "" {
		writeError(w, r, http.StatusBadRequest, "path is required")
		return
	}

	// check valid number of parts
	if params.Parts != nil {
		if *params.Parts < 0 || int32(*params.Parts) > manager.MaxUploadParts {
			writeError(w, r, http.StatusBadRequest, fmt.Sprintf("parts can be between 0 and %d", manager.MaxUploadParts))
			return
		}
	}

	// generate a new address for the object we like to upload
	address := c.PathProvider.NewPath()
	qk, err := c.BlockAdapter.ResolveNamespace(repo.StorageNamespace, address, block.IdentifierTypeRelative)
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}
	err = c.Catalog.SetLinkAddress(ctx, repository, address)
	if c.handleAPIError(ctx, w, r, err) {
		return
	}

	// create a new multipart upload
	mpuResp, err := c.BlockAdapter.CreateMultiPartUpload(ctx, block.ObjectPointer{
		StorageNamespace: repo.StorageNamespace,
		IdentifierType:   block.IdentifierTypeRelative,
		Identifier:       address,
	}, nil, block.CreateMultiPartUploadOpts{})
	if c.handleAPIError(ctx, w, r, err) {
		return
	}

	// prepare presigned URL, for each part
	var presignedURLs []string
	for i := 0; i < swag.IntValue(params.Parts); i++ {
		// generate a pre-signed PUT url for the given request
		preSignedURL, err := c.BlockAdapter.GetPresignUploadPartURL(ctx, block.ObjectPointer{
			StorageNamespace: repo.StorageNamespace,
			Identifier:       address,
			IdentifierType:   block.IdentifierTypeRelative,
		}, mpuResp.UploadID, i+1)
		if c.handleAPIError(ctx, w, r, err) {
			return
		}
		presignedURLs = append(presignedURLs, preSignedURL)
	}

	// write response
	resp := &apigen.PresignMultipartUpload{
		PhysicalAddress: qk.Format(),
		UploadId:        mpuResp.UploadID,
	}
	if len(presignedURLs) > 0 {
		resp.PresignedUrls = &presignedURLs
	}
	writeResponse(w, r, http.StatusCreated, resp)
}

func (c *Controller) AbortPresignMultipartUpload(w http.ResponseWriter, r *http.Request, body apigen.AbortPresignMultipartUploadJSONRequestBody, repository string, branch string, uploadID string, params apigen.AbortPresignMultipartUploadParams) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.WriteObjectAction,
			Resource: permissions.ObjectArn(repository, params.Path),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "abort_presign_multipart_upload", r, repository, branch, "")

	// check if api is supported
	storageConfig := c.getStorageConfig()
	if !swag.BoolValue(storageConfig.PreSignMultipartUpload) {
		writeError(w, r, http.StatusNotImplemented, "presign multipart upload API is not supported")
		return
	}

	// validation checks
	if uploadID == "" {
		writeError(w, r, http.StatusBadRequest, "upload_id is required")
		return
	}
	if params.Path == "" {
		writeError(w, r, http.StatusBadRequest, "path is required")
		return
	}
	if body.PhysicalAddress == "" {
		writeError(w, r, http.StatusBadRequest, "physical_address is required")
		return
	}

	repo, err := c.Catalog.GetRepository(ctx, repository)
	if c.handleAPIError(ctx, w, r, err) {
		return
	}

	// verify physical address
	physicalAddress, addressType := normalizePhysicalAddress(repo.StorageNamespace, body.PhysicalAddress)
	if addressType != catalog.AddressTypeRelative {
		writeError(w, r, http.StatusBadRequest, "physical address must be relative to the storage namespace")
		return
	}

	if err := c.Catalog.VerifyLinkAddress(ctx, repository, physicalAddress); c.handleAPIError(ctx, w, r, err) {
		return
	}

	if err := c.BlockAdapter.AbortMultiPartUpload(ctx, block.ObjectPointer{
		StorageNamespace: repo.StorageNamespace,
		IdentifierType:   block.IdentifierTypeRelative,
		Identifier:       physicalAddress,
	}, uploadID); c.handleAPIError(ctx, w, r, err) {
		return
	}
	writeResponse(w, r, http.StatusNoContent, nil)
}

func (c *Controller) CompletePresignMultipartUpload(w http.ResponseWriter, r *http.Request, body apigen.CompletePresignMultipartUploadJSONRequestBody, repository string, branch string, uploadID string, params apigen.CompletePresignMultipartUploadParams) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.WriteObjectAction,
			Resource: permissions.ObjectArn(repository, params.Path),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "complete_presign_multipart_upload", r, repository, branch, "")

	// check if api is supported
	storageConfig := c.getStorageConfig()
	if !swag.BoolValue(storageConfig.PreSignMultipartUpload) {
		writeError(w, r, http.StatusNotImplemented, "presign multipart upload API is not supported")
		return
	}

	// validation checks
	if uploadID == "" {
		writeError(w, r, http.StatusBadRequest, "upload_id is required")
		return
	}
	if params.Path == "" {
		writeError(w, r, http.StatusBadRequest, "path is required")
		return
	}
	if body.PhysicalAddress == "" {
		writeError(w, r, http.StatusBadRequest, "physical_address is required")
		return
	}
	if len(body.Parts) == 0 {
		writeError(w, r, http.StatusBadRequest, "parts are required")
		return
	}

	// verify physical address
	repo, err := c.Catalog.GetRepository(ctx, repository)
	if c.handleAPIError(ctx, w, r, err) {
		return
	}

	writeTime := time.Now()
	physicalAddress, addressType := normalizePhysicalAddress(repo.StorageNamespace, body.PhysicalAddress)
	if addressType != catalog.AddressTypeRelative {
		writeError(w, r, http.StatusBadRequest, "physical address must be relative to the storage namespace")
		return
	}

	//  verify it has been saved for linking
	if err := c.Catalog.VerifyLinkAddress(ctx, repository, physicalAddress); c.handleAPIError(ctx, w, r, err) {
		return
	}

	var multipartList []block.MultipartPart
	for _, part := range body.Parts {
		multipartList = append(multipartList, block.MultipartPart{
			PartNumber: part.PartNumber,
			ETag:       part.Etag,
		})
	}

	mpuResp, err := c.BlockAdapter.CompleteMultiPartUpload(ctx, block.ObjectPointer{
		StorageNamespace: repo.StorageNamespace,
		IdentifierType:   block.IdentifierTypeRelative,
		Identifier:       physicalAddress,
	}, uploadID, &block.MultipartUploadCompletion{
		Part: multipartList,
	})
	if c.handleAPIError(ctx, w, r, err) {
		return
	}

	checksum := httputil.StripQuotesAndSpaces(mpuResp.ETag)
	entryBuilder := catalog.NewDBEntryBuilder().
		CommonLevel(false).
		Path(params.Path).
		PhysicalAddress(physicalAddress).
		AddressType(addressType).
		CreationDate(writeTime).
		Size(mpuResp.ContentLength).
		Checksum(checksum).
		ContentType(swag.StringValue(body.ContentType))
	if body.UserMetadata != nil {
		entryBuilder.Metadata(body.UserMetadata.AdditionalProperties)
	}
	entry := entryBuilder.Build()

	err = c.Catalog.CreateEntry(ctx, repo.Name, branch, entry)
	if c.handleAPIError(ctx, w, r, err) {
		return
	}

	metadata := apigen.ObjectUserMetadata{AdditionalProperties: entry.Metadata}
	response := apigen.ObjectStats{
		Checksum:        entry.Checksum,
		ContentType:     swag.String(entry.ContentType),
		Metadata:        &metadata,
		Mtime:           entry.CreationDate.Unix(),
		Path:            entry.Path,
		PathType:        entryTypeObject,
		PhysicalAddress: physicalAddress,
		SizeBytes:       swag.Int64(entry.Size),
	}

	writeResponse(w, r, http.StatusOK, response)
}

func (c *Controller) PrepareGarbageCollectionUncommitted(w http.ResponseWriter, r *http.Request, body apigen.PrepareGarbageCollectionUncommittedJSONRequestBody, repository string) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.PrepareGarbageCollectionUncommittedAction,
			Resource: permissions.RepoArn(repository),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "prepare_garbage_collection_uncommitted", r, repository, "", "")

	continuationToken := swag.StringValue(body.ContinuationToken)
	mark, err := decodeGCUncommittedMark(continuationToken)
	if err != nil {
		c.Logger.WithError(err).
			WithFields(logging.Fields{"repository": repository, "continuation_token": continuationToken}).
			Error("Failed to decode gc uncommitted continuation token")
		writeError(w, r, http.StatusBadRequest, "invalid continuation token")
		return
	}

	uncommittedInfo, err := c.Catalog.PrepareGCUncommitted(ctx, repository, mark)
	if err != nil {
		c.Logger.WithError(err).
			WithFields(logging.Fields{"repository": repository, "continuation_token": continuationToken}).
			Error("PrepareGCUncommitted failed")
		c.handleAPIError(ctx, w, r, err)
		return
	}

	nextContinuationToken, err := encodeGCUncommittedMark(uncommittedInfo.Mark)
	if err != nil {
		c.Logger.WithError(err).
			WithFields(logging.Fields{
				"repository": repository,
				"mark":       spew.Sdump(uncommittedInfo.Mark),
			}).
			Error("Failed encoding uncommitted gc mark")
		writeError(w, r, http.StatusInternalServerError, "failed to encode uncommitted mark")
		return
	}

	writeResponse(w, r, http.StatusCreated, apigen.PrepareGCUncommittedResponse{
		RunId:                 uncommittedInfo.RunID,
		GcUncommittedLocation: uncommittedInfo.Location,
		ContinuationToken:     nextContinuationToken,
	})
}

func (c *Controller) GetAuthCapabilities(w http.ResponseWriter, r *http.Request) {
	_, inviteSupported := c.Auth.(auth.EmailInviter)
	writeResponse(w, r, http.StatusOK, apigen.AuthCapabilities{
		InviteUser: &inviteSupported,
	})
}

func (c *Controller) DeleteObjects(w http.ResponseWriter, r *http.Request, body apigen.DeleteObjectsJSONRequestBody, repository, branch string, params apigen.DeleteObjectsParams) {
	ctx := r.Context()
	c.LogAction(ctx, "delete_objects", r, repository, branch, "")

	// limit check
	if len(body.Paths) > DefaultMaxDeleteObjects {
		err := fmt.Errorf("%w, max paths is set to %d", ErrRequestSizeExceeded, DefaultMaxDeleteObjects)
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}

	// errs used to collect errors as part of the response, can't be nil
	errs := make([]apigen.ObjectError, 0)
	// check if we authorize to delete each object, prepare a list of paths we can delete
	var pathsToDelete []string
	for _, objectPath := range body.Paths {
		if !c.authorize(w, r, permissions.Node{
			Permission: permissions.Permission{
				Action:   permissions.DeleteObjectAction,
				Resource: permissions.ObjectArn(repository, objectPath),
			},
		}) {
			errs = append(errs, apigen.ObjectError{
				Path:       swag.String(objectPath),
				StatusCode: http.StatusUnauthorized,
				Message:    http.StatusText(http.StatusUnauthorized),
			})
		} else {
			pathsToDelete = append(pathsToDelete, objectPath)
		}
	}

	// batch delete the entries we allow to delete
	delErr := c.Catalog.DeleteEntries(ctx, repository, branch, pathsToDelete, graveler.WithForce(swag.BoolValue(params.Force)))
	delErrs := graveler.NewMapDeleteErrors(delErr)
	for _, objectPath := range pathsToDelete {
		// set err to the specific error when possible
		err := delErrs[objectPath]
		if err == nil {
			err = delErr
		}
		lg := c.Logger.WithField("path", objectPath)
		switch {
		case errors.Is(err, graveler.ErrNotFound):
			lg.WithError(err).Debug("tried to delete a non-existent object")
		case errors.Is(err, graveler.ErrWriteToProtectedBranch),
			errors.Is(err, graveler.ErrReadOnlyRepository):
			errs = append(errs, apigen.ObjectError{
				Path:       swag.String(objectPath),
				StatusCode: http.StatusForbidden,
				Message:    err.Error(),
			})
		case errors.Is(err, catalog.ErrPathRequiredValue):
			// issue #1706 - https://github.com/treeverse/lakeFS/issues/1706
			// Spark trying to delete the path "main/", which we map to branch "main" with an empty path.
			// Spark expects it to succeed (not deleting anything is a success), instead of returning an error.
			lg.Debug("tried to delete with an empty branch")
		case err != nil:
			lg.WithError(err).Error("failed deleting object")
			errs = append(errs, apigen.ObjectError{
				Path:       swag.String(objectPath),
				StatusCode: http.StatusInternalServerError,
				Message:    err.Error(),
			})
		default:
			lg.Debug("object set for deletion")
		}
	}

	response := apigen.ObjectErrorList{
		Errors: errs,
	}
	writeResponse(w, r, http.StatusOK, response)
}

func (c *Controller) Login(w http.ResponseWriter, r *http.Request, body apigen.LoginJSONRequestBody) {
	ctx := r.Context()
	user, err := userByAuth(ctx, c.Logger, c.Authenticator, c.Auth, body.AccessKeyId, body.SecretAccessKey)
	if errors.Is(err, ErrAuthenticatingRequest) {
		writeResponse(w, r, http.StatusUnauthorized, http.StatusText(http.StatusUnauthorized))
		return
	}

	loginTime := time.Now()
	duration := c.Config.Auth.LoginDuration
	expires := loginTime.Add(duration)
	secret := c.Auth.SecretStore().SharedSecret()

	tokenString, err := GenerateJWTLogin(secret, user.Username, loginTime.Unix(), expires.Unix())
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, http.StatusText(http.StatusInternalServerError))
		return
	}
	internalAuthSession, _ := c.sessionStore.Get(r, InternalAuthSessionName)
	internalAuthSession.Values[TokenSessionKeyName] = tokenString
	err = c.sessionStore.Save(r, w, internalAuthSession)
	if err != nil {
		c.Logger.WithError(err).Error("Failed to save internal auth session")
		writeError(w, r, http.StatusInternalServerError, http.StatusText(http.StatusInternalServerError))
		return
	}
	response := apigen.AuthenticationToken{
		Token:           tokenString,
		TokenExpiration: swag.Int64(expires.Unix()),
	}
	writeResponse(w, r, http.StatusOK, response)
}

func (c *Controller) STSLogin(w http.ResponseWriter, r *http.Request, body apigen.STSLoginJSONRequestBody) {
	ctx := r.Context()
	responseData, err := c.Authentication.ValidateSTS(ctx, body.Code, body.RedirectUri, body.State)
	if c.handleAPIError(ctx, w, r, err) {
		return
	}
	// validate a user exists with the external user id
	_, err = c.Auth.GetUserByExternalID(ctx, responseData.ExternalUserID)
	if c.handleAPIError(ctx, w, r, err) {
		return
	}
	token, err := GenerateJWTLogin(c.Auth.SecretStore().SharedSecret(), responseData.ExternalUserID, time.Now().Unix(), responseData.ExpiresAtUnixTime)
	if c.handleAPIError(ctx, w, r, err) {
		return
	}
	writeResponse(w, r, http.StatusOK, token)
}

func (c *Controller) GetPhysicalAddress(w http.ResponseWriter, r *http.Request, repository, branch string, params apigen.GetPhysicalAddressParams) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.WriteObjectAction,
			Resource: permissions.ObjectArn(repository, params.Path),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "generate_physical_address", r, repository, branch, "")

	repo, err := c.Catalog.GetRepository(ctx, repository)
	if errors.Is(err, graveler.ErrNotFound) {
		writeError(w, r, http.StatusNotFound, err)
		return
	}
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}
	address := c.PathProvider.NewPath()
	qk, err := c.BlockAdapter.ResolveNamespace(repo.StorageNamespace, address, block.IdentifierTypeRelative)
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}
	err = c.Catalog.SetLinkAddress(ctx, repository, address)
	if err != nil {
		c.handleAPIError(ctx, w, r, err)
		return
	}

	response := &apigen.StagingLocation{
		PhysicalAddress: swag.String(qk.Format()),
	}

	if swag.BoolValue(params.Presign) {
		// generate a pre-signed PUT url for the given request
		preSignedURL, expiry, err := c.BlockAdapter.GetPreSignedURL(ctx, block.ObjectPointer{
			StorageNamespace: repo.StorageNamespace,
			Identifier:       address,
			IdentifierType:   block.IdentifierTypeRelative,
		}, block.PreSignModeWrite)
		if err != nil {
			writeError(w, r, http.StatusInternalServerError, err)
			return
		}
		response.PresignedUrl = &preSignedURL
		if !expiry.IsZero() {
			response.PresignedUrlExpiry = swag.Int64(expiry.Unix())
		}
	}

	writeResponse(w, r, http.StatusOK, response)
}

func (c *Controller) LinkPhysicalAddress(w http.ResponseWriter, r *http.Request, body apigen.LinkPhysicalAddressJSONRequestBody, repository, branch string, params apigen.LinkPhysicalAddressParams) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.WriteObjectAction,
			Resource: permissions.ObjectArn(repository, params.Path),
		},
	}) {
		return
	}

	ctx := r.Context()
	c.LogAction(ctx, "stage_object", r, repository, branch, "")

	repo, err := c.Catalog.GetRepository(ctx, repository)
	if errors.Is(err, graveler.ErrNotFound) {
		writeError(w, r, http.StatusNotFound, err)
		return
	}
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}
	// write metadata
	qk, err := c.BlockAdapter.ResolveNamespace(repo.StorageNamespace, params.Path, block.IdentifierTypeRelative)
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}

	ifAbsent := false
	if params.IfNoneMatch != nil {
		if swag.StringValue((*string)(params.IfNoneMatch)) != "*" {
			writeError(w, r, http.StatusBadRequest, "Unsupported value for If-None-Match - Only \"*\" is supported")
			return
		}
		ifAbsent = true
	}

	blockStoreType := c.BlockAdapter.BlockstoreType()
	expectedType := qk.GetStorageType().BlockstoreType()
	if expectedType != blockStoreType {
		c.Logger.WithContext(ctx).WithFields(logging.Fields{
			"expected_type":   expectedType,
			"blockstore_type": blockStoreType,
		}).Error("invalid blockstore type")
		c.handleAPIError(ctx, w, r, fmt.Errorf("invalid blockstore type: %w", block.ErrInvalidAddress))
		return
	}

	writeTime := time.Now()
	fullPhysicalAddress := swag.StringValue(body.Staging.PhysicalAddress)
	physicalAddress, addressType := normalizePhysicalAddress(repo.StorageNamespace, fullPhysicalAddress)

	if addressType == catalog.AddressTypeRelative {
		// if the address is in the storage namespace, verify it has been saved for linking
		err = c.Catalog.VerifyLinkAddress(ctx, repository, physicalAddress)
		if c.handleAPIError(ctx, w, r, err) {
			return
		}
	}

	// trim spaces and quotes from etag
	checksum := httputil.StripQuotesAndSpaces(body.Checksum)
	if checksum == "" {
		writeError(w, r, http.StatusBadRequest, "checksum is required")
		return
	}

	entryBuilder := catalog.NewDBEntryBuilder().
		CommonLevel(false).
		Path(params.Path).
		PhysicalAddress(physicalAddress).
		AddressType(addressType).
		CreationDate(writeTime).
		Size(body.SizeBytes).
		Checksum(checksum).
		ContentType(swag.StringValue(body.ContentType))
	if body.UserMetadata != nil {
		entryBuilder.Metadata(body.UserMetadata.AdditionalProperties)
	}
	entry := entryBuilder.Build()
	err = c.Catalog.CreateEntry(ctx, repo.Name, branch, entry, graveler.WithForce(swag.BoolValue(body.Force)), graveler.WithIfAbsent(ifAbsent))
	if c.handleAPIError(ctx, w, r, err) {
		return
	}

	metadata := apigen.ObjectUserMetadata{AdditionalProperties: entry.Metadata}
	response := apigen.ObjectStats{
		Checksum:        entry.Checksum,
		ContentType:     swag.String(entry.ContentType),
		Metadata:        &metadata,
		Mtime:           entry.CreationDate.Unix(),
		Path:            entry.Path,
		PathType:        entryTypeObject,
		PhysicalAddress: fullPhysicalAddress,
		SizeBytes:       swag.Int64(entry.Size),
	}

	writeResponse(w, r, http.StatusOK, response)
}

// normalizePhysicalAddress return relative address based on storage namespace if possible. If address doesn't match
// the storage namespace prefix, the return address type is full.
func normalizePhysicalAddress(storageNamespace, physicalAddress string) (string, catalog.AddressType) {
	prefix := storageNamespace
	if !strings.HasSuffix(prefix, catalog.DefaultPathDelimiter) {
		prefix += catalog.DefaultPathDelimiter
	}
	if strings.HasPrefix(physicalAddress, prefix) {
		return physicalAddress[len(prefix):], catalog.AddressTypeRelative
	}
	return physicalAddress, catalog.AddressTypeFull
}

func (c *Controller) ListGroups(w http.ResponseWriter, r *http.Request, params apigen.ListGroupsParams) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.ListGroupsAction,
			Resource: permissions.All,
		},
	}) {
		return
	}

	ctx := r.Context()
	c.LogAction(ctx, "list_groups", r, "", "", "")
	groups, paginator, err := c.Auth.ListGroups(ctx, &model.PaginationParams{
		After:  paginationAfter(params.After),
		Prefix: paginationPrefix(params.Prefix),
		Amount: paginationAmount(params.Amount),
	})
	if c.handleAPIError(ctx, w, r, err) {
		return
	}

	response := apigen.GroupList{
		Results: make([]apigen.Group, 0, len(groups)),
		Pagination: apigen.Pagination{
			HasMore:    paginator.NextPageToken != "",
			NextOffset: paginator.NextPageToken,
			Results:    paginator.Amount,
		},
	}

	for _, g := range groups {
		response.Results = append(response.Results, apigen.Group{
			Id:           g.ID,
			Name:         swag.String(g.DisplayName),
			CreationDate: g.CreatedAt.Unix(),
		})
	}
	writeResponse(w, r, http.StatusOK, response)
}

func (c *Controller) CreateGroup(w http.ResponseWriter, r *http.Request, body apigen.CreateGroupJSONRequestBody) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.CreateGroupAction,
			Resource: permissions.GroupArn(body.Id),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "create_group", r, "", "", "")

	// Check that group name is valid
	valid, msg := c.isNameValid(body.Id, "Group")
	if !valid {
		writeError(w, r, http.StatusBadRequest, msg)
		return
	}

	g := &model.Group{
		CreatedAt:   time.Now().UTC(),
		DisplayName: body.Id,
	}
	createdGroup, err := c.Auth.CreateGroup(ctx, g)
	if c.handleAPIError(ctx, w, r, err) {
		return
	}
	response := apigen.Group{
		CreationDate: createdGroup.CreatedAt.Unix(),
		Name:         swag.String(createdGroup.DisplayName),
		Id:           createdGroup.ID,
	}
	writeResponse(w, r, http.StatusCreated, response)
}

func (c *Controller) DeleteGroup(w http.ResponseWriter, r *http.Request, groupID string) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.DeleteGroupAction,
			Resource: permissions.GroupArn(groupID),
		},
	}) {
		return
	}

	ctx := r.Context()
	c.LogAction(ctx, "delete_group", r, "", "", "")
	err := c.Auth.DeleteGroup(ctx, groupID)
	if errors.Is(err, auth.ErrNotFound) {
		writeError(w, r, http.StatusNotFound, "group not found")
		return
	}
	if c.handleAPIError(ctx, w, r, err) {
		return
	}
	writeResponse(w, r, http.StatusNoContent, nil)
}

func (c *Controller) GetGroup(w http.ResponseWriter, r *http.Request, groupID string) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.ReadGroupAction,
			Resource: permissions.GroupArn(groupID),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "get_group", r, "", "", "")
	g, err := c.Auth.GetGroup(ctx, groupID)
	if errors.Is(err, auth.ErrNotFound) {
		writeError(w, r, http.StatusNotFound, "group not found")
		return
	}
	if c.handleAPIError(ctx, w, r, err) {
		return
	}

	response := apigen.Group{
		Id:           g.DisplayName,
		CreationDate: g.CreatedAt.Unix(),
	}
	writeResponse(w, r, http.StatusOK, response)
}

func (c *Controller) GetGroupACL(w http.ResponseWriter, r *http.Request, groupID string) {
	aclPolicyName := acl.PolicyName(groupID)
	if !c.authorize(w, r, permissions.Node{
		Type: permissions.NodeTypeAnd,
		Nodes: []permissions.Node{
			{
				Permission: permissions.Permission{
					Action:   permissions.ReadGroupAction,
					Resource: permissions.GroupArn(groupID),
				},
			},
			{
				Permission: permissions.Permission{
					Action:   permissions.ReadPolicyAction,
					Resource: permissions.PolicyArn(aclPolicyName),
				},
			},
		},
	}) {
		return
	}

	ctx := r.Context()
	c.LogAction(ctx, "get_group_acl", r, "", "", "")
	policies, _, err := c.Auth.ListGroupPolicies(ctx, groupID, &model.PaginationParams{
		Amount: 2, //nolint:gomnd
	})
	if c.handleAPIError(ctx, w, r, err) {
		return
	}

	var groupACL model.ACL
	switch len(policies) {
	case 0: // Blank ACL is valid and allows nothing
		break
	case 1:
		groupACL = policies[0].ACL
		if len(groupACL.Permission) == 0 {
			c.Logger.
				WithContext(ctx).
				WithField("policy", fmt.Sprintf("%+v", policies[0])).
				WithField("acl", fmt.Sprintf("%+v", groupACL)).
				WithField("group", groupID).
				Warn("Policy attached to group has no ACL")
			response := apigen.NotFoundOrNoACL{
				Message: "Policy attached to group has no ACL",
				NoAcl:   swag.Bool(true),
			}
			writeResponse(w, r, http.StatusNotFound, response)
			return
		}
	default:
		c.Logger.
			WithContext(ctx).
			WithField("num_policies", len(policies)).
			WithField("group", groupID).
			Warn("Wrong number of policies found")
		response := apigen.NotFoundOrNoACL{
			Message: "Multiple policies attached to group - no ACL",
			NoAcl:   swag.Bool(true),
		}
		writeResponse(w, r, http.StatusNotFound, response)
		return
	}

	response := apigen.ACL{
		Permission: string(groupACL.Permission),
	}

	writeResponse(w, r, http.StatusOK, response)
}

func (c *Controller) SetGroupACL(w http.ResponseWriter, r *http.Request, body apigen.SetGroupACLJSONRequestBody, groupID string) {
	aclPolicyName := acl.PolicyName(groupID)
	if !c.authorize(w, r, permissions.Node{
		Type: permissions.NodeTypeAnd,
		Nodes: []permissions.Node{
			{
				Permission: permissions.Permission{
					Action:   permissions.ReadGroupAction,
					Resource: permissions.GroupArn(groupID),
				},
			},
			{
				Permission: permissions.Permission{
					Action:   permissions.AttachPolicyAction,
					Resource: permissions.PolicyArn(aclPolicyName),
				},
			},
			{
				Permission: permissions.Permission{
					Action:   permissions.UpdatePolicyAction,
					Resource: permissions.PolicyArn(aclPolicyName),
				},
			},
		},
	}) {
		return
	}

	ctx := r.Context()
	c.LogAction(ctx, "set_group_acl", r, "", "", "")

	newACL := model.ACL{
		Permission: model.ACLPermission(body.Permission),
	}

	err := acl.WriteGroupACL(ctx, c.Auth, groupID, newACL, time.Now(), false)
	if c.handleAPIError(ctx, w, r, err) {
		return
	}

	writeResponse(w, r, http.StatusCreated, nil)
}

func (c *Controller) ListGroupMembers(w http.ResponseWriter, r *http.Request, groupID string, params apigen.ListGroupMembersParams) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.ReadGroupAction,
			Resource: permissions.GroupArn(groupID),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "list_group_users", r, "", "", "")
	users, paginator, err := c.Auth.ListGroupUsers(ctx, groupID, &model.PaginationParams{
		After:  paginationAfter(params.After),
		Prefix: paginationPrefix(params.Prefix),
		Amount: paginationAmount(params.Amount),
	})
	if c.handleAPIError(ctx, w, r, err) {
		return
	}

	response := apigen.UserList{
		Results: make([]apigen.User, 0, len(users)),
		Pagination: apigen.Pagination{
			HasMore:    paginator.NextPageToken != "",
			NextOffset: paginator.NextPageToken,
			Results:    paginator.Amount,
		},
	}
	for _, u := range users {
		response.Results = append(response.Results, apigen.User{
			Id:           u.Username,
			Email:        u.Email,
			CreationDate: u.CreatedAt.Unix(),
		})
	}
	writeResponse(w, r, http.StatusOK, response)
}

func (c *Controller) DeleteGroupMembership(w http.ResponseWriter, r *http.Request, groupID, userID string) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.RemoveGroupMemberAction,
			Resource: permissions.GroupArn(groupID),
		},
	}) {
		return
	}

	ctx := r.Context()
	c.LogAction(ctx, "remove_user_from_group", r, "", "", "")
	err := c.Auth.RemoveUserFromGroup(ctx, userID, groupID)
	if c.handleAPIError(ctx, w, r, err) {
		return
	}
	writeResponse(w, r, http.StatusNoContent, nil)
}

func (c *Controller) AddGroupMembership(w http.ResponseWriter, r *http.Request, groupID, userID string) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.AddGroupMemberAction,
			Resource: permissions.GroupArn(groupID),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "add_user_to_group", r, "", "", "")
	err := c.Auth.AddUserToGroup(ctx, userID, groupID)
	if c.handleAPIError(ctx, w, r, err) {
		return
	}
	writeResponse(w, r, http.StatusCreated, nil)
}

func (c *Controller) ListGroupPolicies(w http.ResponseWriter, r *http.Request, groupID string, params apigen.ListGroupPoliciesParams) {
	if c.Config.IsAuthUISimplified() {
		writeError(w, r, http.StatusNotImplemented, "Not implemented")
		return
	}
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.ReadGroupAction,
			Resource: permissions.GroupArn(groupID),
		},
	}) {
		return
	}

	ctx := r.Context()
	c.LogAction(ctx, "list_group_policies", r, "", "", "")
	policies, paginator, err := c.Auth.ListGroupPolicies(ctx, groupID, &model.PaginationParams{
		After:  paginationAfter(params.After),
		Prefix: paginationPrefix(params.Prefix),
		Amount: paginationAmount(params.Amount),
	})
	if c.handleAPIError(ctx, w, r, err) {
		return
	}

	response := apigen.PolicyList{
		Results: make([]apigen.Policy, 0, len(policies)),
		Pagination: apigen.Pagination{
			HasMore:    paginator.NextPageToken != "",
			NextOffset: paginator.NextPageToken,
			Results:    paginator.Amount,
		},
	}
	for _, p := range policies {
		response.Results = append(response.Results, serializePolicy(p))
	}

	writeResponse(w, r, http.StatusOK, response)
}

func serializePolicy(p *model.Policy) apigen.Policy {
	stmts := make([]apigen.Statement, 0, len(p.Statement))
	for _, s := range p.Statement {
		stmts = append(stmts, apigen.Statement{
			Action:   s.Action,
			Effect:   s.Effect,
			Resource: s.Resource,
		})
	}
	createdAt := p.CreatedAt.Unix()
	return apigen.Policy{
		Id:           p.DisplayName,
		CreationDate: &createdAt, // TODO(barak): check if CreationDate should be required
		Statement:    stmts,
	}
}

func (c *Controller) DetachPolicyFromGroup(w http.ResponseWriter, r *http.Request, groupID, policyID string) {
	if c.Config.IsAuthUISimplified() {
		writeError(w, r, http.StatusNotImplemented, "Not implemented")
		return
	}
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.DetachPolicyAction,
			Resource: permissions.GroupArn(groupID),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "detach_policy_from_group", r, "", "", "")
	err := c.Auth.DetachPolicyFromGroup(ctx, policyID, groupID)
	if c.handleAPIError(ctx, w, r, err) {
		return
	}
	writeResponse(w, r, http.StatusNoContent, nil)
}

func (c *Controller) AttachPolicyToGroup(w http.ResponseWriter, r *http.Request, groupID, policyID string) {
	if c.Config.IsAuthUISimplified() {
		writeError(w, r, http.StatusNotImplemented, "Not implemented")
		return
	}
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.AttachPolicyAction,
			Resource: permissions.GroupArn(groupID),
		},
	}) {
		return
	}

	ctx := r.Context()
	c.LogAction(ctx, "attach_policy_to_group", r, "", "", "")
	err := c.Auth.AttachPolicyToGroup(ctx, policyID, groupID)
	if c.handleAPIError(ctx, w, r, err) {
		return
	}
	writeResponse(w, r, http.StatusCreated, nil)
}

func (c *Controller) ListPolicies(w http.ResponseWriter, r *http.Request, params apigen.ListPoliciesParams) {
	if c.Config.IsAuthUISimplified() {
		writeError(w, r, http.StatusNotImplemented, "Not implemented")
		return
	}
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.ListPoliciesAction,
			Resource: permissions.All,
		},
	}) {
		return
	}

	ctx := r.Context()
	c.LogAction(ctx, "list_policies", r, "", "", "")
	policies, paginator, err := c.Auth.ListPolicies(ctx, &model.PaginationParams{
		After:  paginationAfter(params.After),
		Prefix: paginationPrefix(params.Prefix),
		Amount: paginationAmount(params.Amount),
	})
	if c.handleAPIError(ctx, w, r, err) {
		return
	}

	response := apigen.PolicyList{
		Results: make([]apigen.Policy, 0, len(policies)),
		Pagination: apigen.Pagination{
			HasMore:    paginator.NextPageToken != "",
			NextOffset: paginator.NextPageToken,
			Results:    paginator.Amount,
		},
	}
	for _, p := range policies {
		response.Results = append(response.Results, serializePolicy(p))
	}
	writeResponse(w, r, http.StatusOK, response)
}

func (c *Controller) CreatePolicy(w http.ResponseWriter, r *http.Request, body apigen.CreatePolicyJSONRequestBody) {
	if c.Config.IsAuthUISimplified() {
		writeError(w, r, http.StatusNotImplemented, "Not implemented")
		return
	}
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.CreatePolicyAction,
			Resource: permissions.PolicyArn(body.Id),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "create_policy", r, "", "", "")

	// Check that policy ID is valid
	valid, msg := c.isNameValid(body.Id, "Policy")
	if !valid {
		writeError(w, r, http.StatusBadRequest, msg)
		return
	}

	stmts := make(model.Statements, len(body.Statement))
	for i, apiStatement := range body.Statement {
		stmts[i] = model.Statement{
			Effect:   apiStatement.Effect,
			Action:   apiStatement.Action,
			Resource: apiStatement.Resource,
		}
	}

	p := &model.Policy{
		CreatedAt:   time.Now().UTC(),
		DisplayName: body.Id,
		Statement:   stmts,
	}

	err := c.Auth.WritePolicy(ctx, p, false)
	if c.handleAPIError(ctx, w, r, err) {
		return
	}

	writeResponse(w, r, http.StatusCreated, serializePolicy(p))
}

func (c *Controller) DeletePolicy(w http.ResponseWriter, r *http.Request, policyID string) {
	if c.Config.IsAuthUISimplified() {
		writeError(w, r, http.StatusNotImplemented, "Not implemented")
		return
	}
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.DeletePolicyAction,
			Resource: permissions.PolicyArn(policyID),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "delete_policy", r, "", "", "")
	err := c.Auth.DeletePolicy(ctx, policyID)
	if errors.Is(err, auth.ErrNotFound) {
		writeError(w, r, http.StatusNotFound, "policy not found")
		return
	}
	if c.handleAPIError(ctx, w, r, err) {
		return
	}
	writeResponse(w, r, http.StatusNoContent, nil)
}

func (c *Controller) GetPolicy(w http.ResponseWriter, r *http.Request, policyID string) {
	if c.Config.IsAuthUISimplified() {
		writeError(w, r, http.StatusNotImplemented, "Not implemented")
		return
	}
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.ReadPolicyAction,
			Resource: permissions.PolicyArn(policyID),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "get_policy", r, "", "", "")
	p, err := c.Auth.GetPolicy(ctx, policyID)
	if errors.Is(err, auth.ErrNotFound) {
		writeError(w, r, http.StatusNotFound, "policy not found")
		return
	}
	if c.handleAPIError(ctx, w, r, err) {
		return
	}

	response := serializePolicy(p)
	writeResponse(w, r, http.StatusOK, response)
}

func (c *Controller) UpdatePolicy(w http.ResponseWriter, r *http.Request, body apigen.UpdatePolicyJSONRequestBody, policyID string) {
	if c.Config.IsAuthUISimplified() {
		writeError(w, r, http.StatusNotImplemented, "Not implemented")
		return
	}
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.UpdatePolicyAction,
			Resource: permissions.PolicyArn(policyID),
		},
	}) {
		return
	}
	// verify requested policy match the policy document id
	if policyID != body.Id {
		writeError(w, r, http.StatusBadRequest, "can't update policy id")
		return
	}

	ctx := r.Context()
	c.LogAction(ctx, "update_policy", r, "", "", "")

	stmts := make(model.Statements, len(body.Statement))
	for i, apiStatement := range body.Statement {
		stmts[i] = model.Statement{
			Effect:   apiStatement.Effect,
			Action:   apiStatement.Action,
			Resource: apiStatement.Resource,
		}
	}

	p := &model.Policy{
		CreatedAt:   time.Now().UTC(),
		DisplayName: policyID,
		Statement:   stmts,
	}
	err := c.Auth.WritePolicy(ctx, p, true)
	if c.handleAPIError(ctx, w, r, err) {
		return
	}
	response := serializePolicy(p)
	writeResponse(w, r, http.StatusOK, response)
}

func (c *Controller) ListUsers(w http.ResponseWriter, r *http.Request, params apigen.ListUsersParams) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.ListUsersAction,
			Resource: permissions.All,
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "list_users", r, "", "", "")
	users, paginator, err := c.Auth.ListUsers(ctx, &model.PaginationParams{
		After:  paginationAfter(params.After),
		Prefix: paginationPrefix(params.Prefix),
		Amount: paginationAmount(params.Amount),
	})
	if c.handleAPIError(ctx, w, r, err) {
		return
	}

	response := apigen.UserList{
		Results: make([]apigen.User, 0, len(users)),
		Pagination: apigen.Pagination{
			HasMore:    paginator.NextPageToken != "",
			NextOffset: paginator.NextPageToken,
			Results:    paginator.Amount,
		},
	}
	for _, u := range users {
		response.Results = append(response.Results, apigen.User{
			Id:           u.Username,
			Email:        u.Email,
			FriendlyName: u.FriendlyName,
			CreationDate: u.CreatedAt.Unix(),
		})
	}
	writeResponse(w, r, http.StatusOK, response)
}

func (c *Controller) CreateUser(w http.ResponseWriter, r *http.Request, body apigen.CreateUserJSONRequestBody) {
	invite := swag.BoolValue(body.InviteUser)
	username := body.Id

	// Check that username is valid
	valid, msg := c.isNameValid(username, "User")
	if !valid {
		writeError(w, r, http.StatusBadRequest, msg)
		return
	}

	var parsedEmail *string
	if invite {
		// Check that email is valid
		addr, err := mail.ParseAddress(username)
		if err != nil {
			c.Logger.WithError(err).WithField("user_id", username).Warn("failed parsing email")
			writeError(w, r, http.StatusBadRequest, "Invalid email format")
			return
		}
		username = strings.ToLower(addr.Address)
		parsedEmail = &addr.Address
	}
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.CreateUserAction,
			Resource: permissions.UserArn(username),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "create_user", r, "", "", "")
	if invite {
		inviter, ok := c.Auth.(auth.EmailInviter)
		if !ok {
			writeError(w, r, http.StatusNotImplemented, "Not implemented")
			return
		}
		err := inviter.InviteUser(ctx, *parsedEmail)
		if c.handleAPIError(ctx, w, r, err) {
			c.Logger.WithError(err).WithField("email", *parsedEmail).Warn("Failed creating user")
			return
		}
		writeResponse(w, r, http.StatusCreated, apigen.User{Id: *parsedEmail})
		return
	}
	u := &model.User{
		CreatedAt:    time.Now().UTC(),
		Username:     username,
		FriendlyName: nil,
		Source:       "internal",
		Email:        parsedEmail,
	}

	_, err := c.Auth.CreateUser(ctx, u)
	if c.handleAPIError(ctx, w, r, err) {
		c.Logger.WithError(err).WithField("username", u.Username).Warn("failed creating user")
		return
	}
	response := apigen.User{
		Id:           u.Username,
		Email:        u.Email,
		CreationDate: u.CreatedAt.Unix(),
	}
	writeResponse(w, r, http.StatusCreated, response)
}

func (c *Controller) GetUser(w http.ResponseWriter, r *http.Request, userID string) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.ReadUserAction,
			Resource: permissions.UserArn(userID),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "get_user", r, "", "", "")
	u, err := c.Auth.GetUser(ctx, userID)
	if errors.Is(err, auth.ErrNotFound) {
		writeError(w, r, http.StatusNotFound, "user not found")
		return
	}
	if c.handleAPIError(ctx, w, r, err) {
		return
	}
	response := apigen.User{
		CreationDate: u.CreatedAt.Unix(),
		Email:        u.Email,
		Id:           u.Username,
	}
	writeResponse(w, r, http.StatusOK, response)
}

func (c *Controller) ListUserCredentials(w http.ResponseWriter, r *http.Request, userID string, params apigen.ListUserCredentialsParams) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.ListCredentialsAction,
			Resource: permissions.UserArn(userID),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "list_user_credentials", r, "", "", "")
	credentials, paginator, err := c.Auth.ListUserCredentials(ctx, userID, &model.PaginationParams{
		After:  paginationAfter(params.After),
		Prefix: paginationPrefix(params.Prefix),
		Amount: paginationAmount(params.Amount),
	})
	if c.handleAPIError(ctx, w, r, err) {
		return
	}

	response := apigen.CredentialsList{
		Results: make([]apigen.Credentials, 0, len(credentials)),
		Pagination: apigen.Pagination{
			HasMore:    paginator.NextPageToken != "",
			NextOffset: paginator.NextPageToken,
			Results:    paginator.Amount,
		},
	}
	for _, c := range credentials {
		response.Results = append(response.Results, apigen.Credentials{
			AccessKeyId:  c.AccessKeyID,
			CreationDate: c.IssuedDate.Unix(),
		})
	}
	writeResponse(w, r, http.StatusOK, response)
}

func (c *Controller) CreateCredentials(w http.ResponseWriter, r *http.Request, userID string) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.CreateCredentialsAction,
			Resource: permissions.UserArn(userID),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "create_credentials", r, "", "", "")
	credentials, err := c.Auth.CreateCredentials(ctx, userID)
	if c.handleAPIError(ctx, w, r, err) {
		return
	}
	response := apigen.CredentialsWithSecret{
		AccessKeyId:     credentials.AccessKeyID,
		SecretAccessKey: credentials.SecretAccessKey,
		CreationDate:    credentials.IssuedDate.Unix(),
	}
	writeResponse(w, r, http.StatusCreated, response)
}

func (c *Controller) DeleteCredentials(w http.ResponseWriter, r *http.Request, userID, accessKeyID string) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.DeleteCredentialsAction,
			Resource: permissions.UserArn(userID),
		},
	}) {
		return
	}

	ctx := r.Context()
	c.LogAction(ctx, "delete_credentials", r, "", "", "")
	err := c.Auth.DeleteCredentials(ctx, userID, accessKeyID)
	if errors.Is(err, auth.ErrNotFound) {
		writeError(w, r, http.StatusNotFound, "credentials not found")
		return
	}
	if c.handleAPIError(ctx, w, r, err) {
		return
	}
	writeResponse(w, r, http.StatusNoContent, nil)
}

func (c *Controller) GetCredentials(w http.ResponseWriter, r *http.Request, userID, accessKeyID string) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.ReadCredentialsAction,
			Resource: permissions.UserArn(userID),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "get_credentials_for_user", r, "", "", "")
	credentials, err := c.Auth.GetCredentialsForUser(ctx, userID, accessKeyID)
	if errors.Is(err, auth.ErrNotFound) {
		writeError(w, r, http.StatusNotFound, "credentials not found")
		return
	}
	if c.handleAPIError(ctx, w, r, err) {
		return
	}

	response := apigen.Credentials{
		AccessKeyId:  credentials.AccessKeyID,
		CreationDate: credentials.IssuedDate.Unix(),
	}
	writeResponse(w, r, http.StatusOK, response)
}

func (c *Controller) ListUserGroups(w http.ResponseWriter, r *http.Request, userID string, params apigen.ListUserGroupsParams) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.ReadUserAction,
			Resource: permissions.UserArn(userID),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "list_user_groups", r, "", "", "")
	groups, paginator, err := c.Auth.ListUserGroups(ctx, userID, &model.PaginationParams{
		After:  paginationAfter(params.After),
		Prefix: paginationPrefix(params.Prefix),
		Amount: paginationAmount(params.Amount),
	})
	if c.handleAPIError(ctx, w, r, err) {
		return
	}

	response := apigen.GroupList{
		Results: make([]apigen.Group, 0, len(groups)),
		Pagination: apigen.Pagination{
			HasMore:    paginator.NextPageToken != "",
			NextOffset: paginator.NextPageToken,
			Results:    paginator.Amount,
		},
	}
	for _, g := range groups {
		response.Results = append(response.Results, apigen.Group{
			Id:           g.ID,
			Name:         swag.String(g.DisplayName),
			CreationDate: g.CreatedAt.Unix(),
		})
	}

	writeResponse(w, r, http.StatusOK, response)
}

func (c *Controller) ListUserPolicies(w http.ResponseWriter, r *http.Request, userID string, params apigen.ListUserPoliciesParams) {
	if c.Config.IsAuthUISimplified() {
		writeError(w, r, http.StatusNotImplemented, "Not implemented")
		return
	}
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.ReadUserAction,
			Resource: permissions.UserArn(userID),
		},
	}) {
		return
	}

	ctx := r.Context()
	c.LogAction(ctx, "list_user_policies", r, "", "", "")
	var listPolicies func(ctx context.Context, username string, params *model.PaginationParams) ([]*model.Policy, *model.Paginator, error)
	if params.Effective != nil && *params.Effective {
		listPolicies = c.Auth.ListEffectivePolicies
	} else {
		listPolicies = c.Auth.ListUserPolicies
	}
	policies, paginator, err := listPolicies(ctx, userID, &model.PaginationParams{
		After:  paginationAfter(params.After),
		Prefix: paginationPrefix(params.Prefix),
		Amount: paginationAmount(params.Amount),
	})
	if c.handleAPIError(ctx, w, r, err) {
		return
	}

	response := apigen.PolicyList{
		Pagination: apigen.Pagination{
			HasMore:    paginator.NextPageToken != "",
			NextOffset: paginator.NextPageToken,
			Results:    paginator.Amount,
		},
		Results: make([]apigen.Policy, 0, len(policies)),
	}
	for _, p := range policies {
		response.Results = append(response.Results, serializePolicy(p))
	}
	writeResponse(w, r, http.StatusOK, response)
}

func (c *Controller) DetachPolicyFromUser(w http.ResponseWriter, r *http.Request, userID, policyID string) {
	if c.Config.IsAuthUISimplified() {
		writeError(w, r, http.StatusNotImplemented, "Not implemented")
		return
	}
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.DetachPolicyAction,
			Resource: permissions.UserArn(userID),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "detach_policy_from_user", r, "", "", "")
	err := c.Auth.DetachPolicyFromUser(ctx, policyID, userID)
	if c.handleAPIError(ctx, w, r, err) {
		return
	}
	writeResponse(w, r, http.StatusNoContent, nil)
}

func (c *Controller) AttachPolicyToUser(w http.ResponseWriter, r *http.Request, userID, policyID string) {
	if c.Config.IsAuthUISimplified() {
		writeError(w, r, http.StatusNotImplemented, "Not implemented")
		return
	}
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.AttachPolicyAction,
			Resource: permissions.UserArn(userID),
		},
	}) {
		return
	}

	ctx := r.Context()
	c.LogAction(ctx, "attach_policy_to_user", r, "", "", "")
	err := c.Auth.AttachPolicyToUser(ctx, policyID, userID)
	if c.handleAPIError(ctx, w, r, err) {
		return
	}
	writeResponse(w, r, http.StatusCreated, nil)
}

func (c *Controller) GetConfig(w http.ResponseWriter, r *http.Request) {
	_, err := auth.GetUser(r.Context())
	if err != nil {
		writeError(w, r, http.StatusUnauthorized, ErrAuthenticatingRequest)
		return
	}
	var storageCfg apigen.StorageConfig
	internalError := false
	if !c.authorizeCallback(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.ReadConfigAction,
			Resource: permissions.All,
		},
	}, func(_ http.ResponseWriter, _ *http.Request, code int, v interface{}) {
		switch code {
		case http.StatusInternalServerError:
			writeError(w, r, code, v)
			internalError = true
		case http.StatusUnauthorized:
			c.Logger.Debug("Unauthorized request to get storage config, returning partial config")
		}
	}) {
		if internalError {
			return
		}
	} else {
		storageCfg = c.getStorageConfig()
	}

	versionConfig := c.getVersionConfig()
	writeResponse(w, r, http.StatusOK, apigen.Config{StorageConfig: &storageCfg, VersionConfig: &versionConfig})
}

func (c *Controller) GetStorageConfig(w http.ResponseWriter, r *http.Request) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.ReadConfigAction,
			Resource: permissions.All,
		},
	}) {
		return
	}

	writeResponse(w, r, http.StatusOK, c.getStorageConfig())
}

func (c *Controller) getStorageConfig() apigen.StorageConfig {
	info := c.BlockAdapter.GetStorageNamespaceInfo()
	defaultNamespacePrefix := swag.String(info.DefaultNamespacePrefix)
	if c.Config.Blockstore.DefaultNamespacePrefix != nil {
		defaultNamespacePrefix = c.Config.Blockstore.DefaultNamespacePrefix
	}
	return apigen.StorageConfig{
		BlockstoreType:                   c.Config.Blockstore.Type,
		BlockstoreNamespaceValidityRegex: info.ValidityRegex,
		BlockstoreNamespaceExample:       info.Example,
		DefaultNamespacePrefix:           defaultNamespacePrefix,
		PreSignSupport:                   info.PreSignSupport,
		PreSignSupportUi:                 info.PreSignSupportUI,
		ImportSupport:                    info.ImportSupport,
		ImportValidityRegex:              info.ImportValidityRegex,
		PreSignMultipartUpload:           swag.Bool(info.PreSignSupportMultipart),
	}
}

func (c *Controller) HealthCheck(w http.ResponseWriter, r *http.Request) {
	writeResponse(w, r, http.StatusNoContent, nil)
}

func (c *Controller) ListRepositories(w http.ResponseWriter, r *http.Request, params apigen.ListRepositoriesParams) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.ListRepositoriesAction,
			Resource: permissions.All,
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "list_repos", r, "", "", "")

	repos, hasMore, err := c.Catalog.ListRepositories(ctx, paginationAmount(params.Amount), paginationPrefix(params.Prefix), paginationAfter(params.After))
	if c.handleAPIError(ctx, w, r, err) {
		return
	}
	results := make([]apigen.Repository, 0, len(repos))
	for _, repo := range repos {
		creationDate := repo.CreationDate.Unix()
		r := apigen.Repository{
			Id:               repo.Name,
			StorageNamespace: repo.StorageNamespace,
			CreationDate:     creationDate,
			DefaultBranch:    repo.DefaultBranch,
			ReadOnly:         swag.Bool(repo.ReadOnly),
		}
		results = append(results, r)
	}
	repositoryList := apigen.RepositoryList{
		Pagination: paginationFor(hasMore, results, "Id"),
		Results:    results,
	}
	writeResponse(w, r, http.StatusOK, repositoryList)
}

func (c *Controller) CreateRepository(w http.ResponseWriter, r *http.Request, body apigen.CreateRepositoryJSONRequestBody, params apigen.CreateRepositoryParams) {
	if !c.authorize(w, r, permissions.Node{
		Type: permissions.NodeTypeAnd,
		Nodes: []permissions.Node{
			{
				Permission: permissions.Permission{
					Action:   permissions.CreateRepositoryAction,
					Resource: permissions.RepoArn(body.Name),
				},
			},
			{
				Permission: permissions.Permission{
					Action:   permissions.AttachStorageNamespaceAction,
					Resource: permissions.StorageNamespace(body.StorageNamespace),
				},
			},
		},
	}) {
		return
	}
	ctx := r.Context()

	// Verify first if there is a repository definition.
	// Return conflict if definition already exists, before
	// creating the repository itself and ensuring (optional) storage namespace holds an object.
	// Example will be by restoring a repository from a backup or previous bare repository.
	_, err := c.Catalog.GetRepository(ctx, body.Name)
	if err == nil {
		c.handleAPIError(ctx, w, r, fmt.Errorf("error creating repository: %w", graveler.ErrNotUnique))
		return
	}
	sampleData := swag.BoolValue(body.SampleData)
	c.LogAction(ctx, "create_repo", r, body.Name, "", "")
	if sampleData {
		c.LogAction(ctx, "repo_sample_data", r, body.Name, "", "")
	}

	if err := c.validateStorageNamespace(body.StorageNamespace); err != nil {
		writeError(w, r, http.StatusBadRequest, err)
		return
	}

	defaultBranch := swag.StringValue(body.DefaultBranch)
	if defaultBranch == "" {
		defaultBranch = "main"
	}

	if swag.BoolValue(params.Bare) {
		// create a bare repository. This is useful in conjunction with refs-restore to create a copy
		// of another repository by e.g. copying the _lakefs/ directory and restoring its refs
		repo, err := c.Catalog.CreateBareRepository(ctx, body.Name, body.StorageNamespace, defaultBranch, swag.BoolValue(body.ReadOnly))
		if c.handleAPIError(ctx, w, r, err) {
			return
		}
		response := apigen.Repository{
			CreationDate:     repo.CreationDate.Unix(),
			DefaultBranch:    repo.DefaultBranch,
			Id:               repo.Name,
			StorageNamespace: repo.StorageNamespace,
		}
		writeResponse(w, r, http.StatusCreated, response)
		return
	}

	if !swag.BoolValue(body.ReadOnly) {
		if err := c.ensureStorageNamespace(ctx, body.StorageNamespace); err != nil {
			var (
				reason string
				retErr error
				urlErr *url.Error
			)
			switch {
			case errors.As(err, &urlErr) && urlErr.Op == "parse":
				retErr = err
				reason = "bad_url"
			case errors.Is(err, block.ErrInvalidAddress):
				retErr = fmt.Errorf("%w, must match: %s", err, c.BlockAdapter.BlockstoreType())
				reason = "invalid_namespace"
			case errors.Is(err, ErrStorageNamespaceInUse):
				retErr = err
				reason = "already_in_use"
			default:
				retErr = ErrFailedToAccessStorage
				reason = "unknown"
			}
			c.Logger.
				WithError(err).
				WithField("storage_namespace", body.StorageNamespace).
				WithField("reason", reason).
				Warn("Could not access storage namespace")
			writeError(w, r, http.StatusBadRequest, fmt.Errorf("failed to create repository: %w", retErr))
			return
		}
	}

	newRepo, err := c.Catalog.CreateRepository(ctx, body.Name, body.StorageNamespace, defaultBranch, swag.BoolValue(body.ReadOnly))
	if err != nil {
		c.handleAPIError(ctx, w, r, fmt.Errorf("error creating repository: %w", err))
		return
	}

	if sampleData {
		// add sample data, hooks, etc.
		user, err := auth.GetUser(ctx)
		if err != nil {
			writeError(w, r, http.StatusUnauthorized, "missing user")
			return
		}

		err = samplerepo.PopulateSampleRepo(ctx, newRepo, c.Catalog, c.PathProvider, c.BlockAdapter, user)
		if err != nil {
			c.handleAPIError(ctx, w, r, fmt.Errorf("error populating sample repository: %w", err))
			return
		}

		err = samplerepo.AddBranchProtection(ctx, newRepo, c.Catalog)
		if err != nil {
			c.handleAPIError(ctx, w, r, fmt.Errorf("error adding branch protection to sample repository: %w", err))
			return
		}
	}

	response := apigen.Repository{
		CreationDate:     newRepo.CreationDate.Unix(),
		DefaultBranch:    newRepo.DefaultBranch,
		Id:               newRepo.Name,
		StorageNamespace: newRepo.StorageNamespace,
		ReadOnly:         swag.Bool(newRepo.ReadOnly),
	}
	writeResponse(w, r, http.StatusCreated, response)
}

func (c *Controller) validateStorageNamespace(storageNamespace string) error {
	validRegex := c.BlockAdapter.GetStorageNamespaceInfo().ValidityRegex
	storagePrefixRegex, err := regexp.Compile(validRegex)
	if err != nil {
		return fmt.Errorf("failed to compile validity regex %s: %w", validRegex, block.ErrInvalidNamespace)
	}
	if !storagePrefixRegex.MatchString(storageNamespace) {
		return fmt.Errorf("failed to match required regex %s: %w", validRegex, block.ErrInvalidNamespace)
	}
	return nil
}

func (c *Controller) ensureStorageNamespace(ctx context.Context, storageNamespace string) error {
	const (
		dummyData    = "this is dummy data - created by lakeFS to check accessibility"
		dummyObjName = "dummy"
	)
	dummyKey := fmt.Sprintf("%s/%s", c.Config.Committed.BlockStoragePrefix, dummyObjName)

	objLen := int64(len(dummyData))

	// check if the dummy file exist in the root of the storage namespace
	// this serves two purposes, first, we maintain safety check for older lakeFS version.
	// second, in scenarios where lakeFS shouldn't have access to the root namespace (i.e pre-sign URL only).
	if c.Config.Graveler.EnsureReadableRootNamespace {
		rootObj := block.ObjectPointer{
			StorageNamespace: storageNamespace,
			IdentifierType:   block.IdentifierTypeRelative,
			Identifier:       dummyObjName,
		}

		if s, err := c.BlockAdapter.Get(ctx, rootObj, objLen); err == nil {
			_ = s.Close()
			return fmt.Errorf("found lakeFS objects in the storage namespace root(%s): %w",
				storageNamespace, ErrStorageNamespaceInUse)
		} else if !errors.Is(err, block.ErrDataNotFound) {
			return err
		}
	}

	// check if the dummy file exists
	obj := block.ObjectPointer{
		StorageNamespace: storageNamespace,
		IdentifierType:   block.IdentifierTypeRelative,
		Identifier:       dummyKey,
	}

	if s, err := c.BlockAdapter.Get(ctx, obj, objLen); err == nil {
		_ = s.Close()
		return fmt.Errorf("found lakeFS objects in the storage namespace(%s) key(%s): %w",
			storageNamespace, obj.Identifier, ErrStorageNamespaceInUse)
	} else if !errors.Is(err, block.ErrDataNotFound) {
		return err
	}

	if err := c.BlockAdapter.Put(ctx, obj, objLen, strings.NewReader(dummyData), block.PutOpts{}); err != nil {
		return err
	}

	_, err := c.BlockAdapter.Get(ctx, obj, objLen)
	return err
}

func (c *Controller) DeleteRepository(w http.ResponseWriter, r *http.Request, repository string, params apigen.DeleteRepositoryParams) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.DeleteRepositoryAction,
			Resource: permissions.RepoArn(repository),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "delete_repo", r, repository, "", "")
	err := c.Catalog.DeleteRepository(ctx, repository, graveler.WithForce(swag.BoolValue(params.Force)))
	if c.handleAPIError(ctx, w, r, err) {
		return
	}
	writeResponse(w, r, http.StatusNoContent, nil)
}

func (c *Controller) GetRepository(w http.ResponseWriter, r *http.Request, repository string) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.ReadRepositoryAction,
			Resource: permissions.RepoArn(repository),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "get_repo", r, repository, "", "")
	repo, err := c.Catalog.GetRepository(ctx, repository)
	switch {
	case err == nil:
		response := apigen.Repository{
			CreationDate:     repo.CreationDate.Unix(),
			DefaultBranch:    repo.DefaultBranch,
			Id:               repo.Name,
			StorageNamespace: repo.StorageNamespace,
			ReadOnly:         swag.Bool(repo.ReadOnly),
		}
		writeResponse(w, r, http.StatusOK, response)

	case errors.Is(err, graveler.ErrNotFound):
		writeError(w, r, http.StatusNotFound, "repository not found")

	case errors.Is(err, graveler.ErrRepositoryInDeletion):
		writeError(w, r, http.StatusGone, err)

	default:
		writeError(w, r, http.StatusInternalServerError, err)
	}
}

func (c *Controller) GetRepositoryMetadata(w http.ResponseWriter, r *http.Request, repository string) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.ReadRepositoryAction,
			Resource: permissions.RepoArn(repository),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "get_repo_metadata", r, repository, "", "")
	metadata, err := c.Catalog.GetRepositoryMetadata(ctx, repository)
	if c.handleAPIError(ctx, w, r, err) {
		return
	}
	writeResponse(w, r, http.StatusOK, apigen.RepositoryMetadata{AdditionalProperties: metadata})
}

func (c *Controller) SetRepositoryMetadata(w http.ResponseWriter, r *http.Request, body apigen.SetRepositoryMetadataJSONRequestBody, repository string) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.UpdateRepositoryAction,
			Resource: permissions.RepoArn(repository),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "update_repo_metadata", r, repository, "", "")
	err := c.Catalog.UpdateRepositoryMetadata(ctx, repository, body.Metadata.AdditionalProperties)
	if c.handleAPIError(ctx, w, r, err) {
		return
	}
	writeResponse(w, r, http.StatusNoContent, nil)
}

func (c *Controller) DeleteRepositoryMetadata(w http.ResponseWriter, r *http.Request, body apigen.DeleteRepositoryMetadataJSONRequestBody, repository string) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.UpdateRepositoryAction,
			Resource: permissions.RepoArn(repository),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "delete_repo_metadata", r, repository, "", "")
	err := c.Catalog.DeleteRepositoryMetadata(ctx, repository, body.Keys)
	if c.handleAPIError(ctx, w, r, err) {
		return
	}
	writeResponse(w, r, http.StatusNoContent, nil)
}

func (c *Controller) GetBranchProtectionRules(w http.ResponseWriter, r *http.Request, repository string) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.GetBranchProtectionRulesAction,
			Resource: permissions.RepoArn(repository),
		},
	}) {
		return
	}
	ctx := r.Context()
	rules, eTag, err := c.Catalog.GetBranchProtectionRules(ctx, repository)
	if c.handleAPIError(ctx, w, r, err) {
		return
	}
	resp := make([]*apigen.BranchProtectionRule, 0, len(rules.BranchPatternToBlockedActions))
	for pattern := range rules.BranchPatternToBlockedActions {
		resp = append(resp, &apigen.BranchProtectionRule{
			Pattern: pattern,
		})
	}
	w.Header().Set("ETag", swag.StringValue(eTag))
	writeResponse(w, r, http.StatusOK, resp)
}

func (c *Controller) SetBranchProtectionRules(w http.ResponseWriter, r *http.Request, body apigen.SetBranchProtectionRulesJSONRequestBody, repository string, params apigen.SetBranchProtectionRulesParams) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.SetBranchProtectionRulesAction,
			Resource: permissions.RepoArn(repository),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "create_branch_protection_rule", r, repository, "", "")

	// For now, all protected branches use the same default set of blocked actions. In the future this set will be user configurable.
	blockedActions := []graveler.BranchProtectionBlockedAction{graveler.BranchProtectionBlockedAction_STAGING_WRITE, graveler.BranchProtectionBlockedAction_COMMIT}

	rules := &graveler.BranchProtectionRules{
		BranchPatternToBlockedActions: make(map[string]*graveler.BranchProtectionBlockedActions),
	}
	for _, r := range body {
		rules.BranchPatternToBlockedActions[r.Pattern] = &graveler.BranchProtectionBlockedActions{
			Value: blockedActions,
		}
	}
	err := c.Catalog.SetBranchProtectionRules(ctx, repository, rules, params.IfMatch)
	if c.handleAPIError(ctx, w, r, err) {
		return
	}
	writeResponse(w, r, http.StatusNoContent, nil)
}

func (c *Controller) DeleteGCRules(w http.ResponseWriter, r *http.Request, repository string) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.SetGarbageCollectionRulesAction,
			Resource: permissions.RepoArn(repository),
		},
	}) {
		return
	}
	ctx := r.Context()
	err := c.Catalog.SetGarbageCollectionRules(ctx, repository, nil)
	if c.handleAPIError(ctx, w, r, err) {
		return
	}
	writeResponse(w, r, http.StatusNoContent, nil)
}

func (c *Controller) GetGCRules(w http.ResponseWriter, r *http.Request, repository string) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.GetGarbageCollectionRulesAction,
			Resource: permissions.RepoArn(repository),
		},
	}) {
		return
	}
	ctx := r.Context()
	rules, err := c.Catalog.GetGarbageCollectionRules(ctx, repository)
	if c.handleAPIError(ctx, w, r, err) {
		return
	}
	resp := apigen.GarbageCollectionRules{}
	resp.DefaultRetentionDays = int(rules.DefaultRetentionDays)
	for branchID, retentionDays := range rules.BranchRetentionDays {
		resp.Branches = append(resp.Branches, apigen.GarbageCollectionRule{BranchId: branchID, RetentionDays: int(retentionDays)})
	}
	writeResponse(w, r, http.StatusOK, resp)
}

func (c *Controller) SetGCRules(w http.ResponseWriter, r *http.Request, body apigen.SetGCRulesJSONRequestBody, repository string) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.SetGarbageCollectionRulesAction,
			Resource: permissions.RepoArn(repository),
		},
	}) {
		return
	}
	ctx := r.Context()
	rules := &graveler.GarbageCollectionRules{
		DefaultRetentionDays: int32(body.DefaultRetentionDays),
		BranchRetentionDays:  make(map[string]int32),
	}
	for _, rule := range body.Branches {
		rules.BranchRetentionDays[rule.BranchId] = int32(rule.RetentionDays)
	}
	err := c.Catalog.SetGarbageCollectionRules(ctx, repository, rules)
	if c.handleAPIError(ctx, w, r, err) {
		return
	}
	writeResponse(w, r, http.StatusNoContent, nil)
}

func (c *Controller) ListRepositoryRuns(w http.ResponseWriter, r *http.Request, repository string, params apigen.ListRepositoryRunsParams) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.ReadActionsAction,
			Resource: permissions.RepoArn(repository),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "actions_repository_runs", r, repository, "", "")

	_, err := c.Catalog.GetRepository(ctx, repository)
	if c.handleAPIError(ctx, w, r, err) {
		return
	}

	branchName := swag.StringValue(params.Branch)
	commitID := swag.StringValue(params.Commit)
	runsIter, err := c.Actions.ListRunResults(ctx, repository, branchName, commitID, paginationAfter(params.After))
	if c.handleAPIError(ctx, w, r, err) {
		return
	}
	defer runsIter.Close()

	response := apigen.ActionRunList{
		Pagination: apigen.Pagination{
			MaxPerPage: DefaultMaxPerPage,
		},
		Results: make([]apigen.ActionRun, 0),
	}
	amount := paginationAmount(params.Amount)
	for len(response.Results) < amount && runsIter.Next() {
		val := runsIter.Value()
		response.Results = append(response.Results, runResultToActionRun(val))
	}
	response.Pagination.Results = len(response.Results)
	if runsIter.Next() {
		response.Pagination.HasMore = true
		if len(response.Results) > 0 {
			lastRun := response.Results[len(response.Results)-1]
			response.Pagination.NextOffset = lastRun.RunId
		}
	}
	if err := runsIter.Err(); err != nil {
		writeResponse(w, r, http.StatusInternalServerError, err)
		return
	}
	writeResponse(w, r, http.StatusOK, response)
}

func runResultToActionRun(val *actions.RunResult) apigen.ActionRun {
	runResult := apigen.ActionRun{
		Branch:    val.BranchID,
		CommitId:  val.CommitID,
		RunId:     val.RunID,
		StartTime: val.StartTime,
		EndTime:   &val.EndTime,
		EventType: val.EventType,
	}
	if val.Passed {
		runResult.Status = actionStatusCompleted
	} else {
		runResult.Status = actionStatusFailed
	}
	return runResult
}

func (c *Controller) GetRun(w http.ResponseWriter, r *http.Request, repository, runID string) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.ReadActionsAction,
			Resource: permissions.RepoArn(repository),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "actions_get_run", r, repository, "", "")
	_, err := c.Catalog.GetRepository(ctx, repository)
	if c.handleAPIError(ctx, w, r, err) {
		return
	}

	runResult, err := c.Actions.GetRunResult(ctx, repository, runID)
	if c.handleAPIError(ctx, w, r, err) {
		return
	}

	var status string
	if runResult.Passed {
		status = actionStatusCompleted
	} else {
		status = actionStatusFailed
	}
	response := apigen.ActionRun{
		RunId:     runResult.RunID,
		EventType: runResult.EventType,
		StartTime: runResult.StartTime,
		EndTime:   &runResult.EndTime,
		Status:    status,
		Branch:    runResult.BranchID,
		CommitId:  runResult.CommitID,
	}
	writeResponse(w, r, http.StatusOK, response)
}

func (c *Controller) ListRunHooks(w http.ResponseWriter, r *http.Request, repository, runID string, params apigen.ListRunHooksParams) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.ReadActionsAction,
			Resource: permissions.RepoArn(repository),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "actions_list_run_hooks", r, repository, "", "")

	repo, err := c.Catalog.GetRepository(ctx, repository)
	if c.handleAPIError(ctx, w, r, err) {
		return
	}

	tasksIter, err := c.Actions.ListRunTaskResults(ctx, repo.Name, runID, paginationAfter(params.After))
	if c.handleAPIError(ctx, w, r, err) {
		return
	}
	defer tasksIter.Close()

	response := apigen.HookRunList{
		Results: make([]apigen.HookRun, 0),
		Pagination: apigen.Pagination{
			MaxPerPage: DefaultMaxPerPage,
		},
	}
	amount := paginationAmount(params.Amount)
	for len(response.Results) < amount && tasksIter.Next() {
		val := tasksIter.Value()
		hookRun := apigen.HookRun{
			HookRunId: val.HookRunID,
			Action:    val.ActionName,
			HookId:    val.HookID,
			StartTime: val.StartTime,
			EndTime:   &val.EndTime,
		}
		switch {
		case val.Passed:
			hookRun.Status = actionStatusCompleted
		case val.StartTime.IsZero(): // assumes that database values are only stored after run is finished
			hookRun.Status = actionStatusSkipped
			hookRun.EndTime = nil
		default:
			hookRun.Status = actionStatusFailed
		}
		response.Results = append(response.Results, hookRun)
	}
	if tasksIter.Next() {
		response.Pagination.HasMore = true
		if len(response.Results) > 0 {
			lastHookRun := response.Results[len(response.Results)-1]
			response.Pagination.NextOffset = lastHookRun.HookRunId
		}
	}
	if err := tasksIter.Err(); err != nil {
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}
	response.Pagination.Results = len(response.Results)
	writeResponse(w, r, http.StatusOK, response)
}

func (c *Controller) GetRunHookOutput(w http.ResponseWriter, r *http.Request, repository, runID, hookRunID string) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.ReadActionsAction,
			Resource: permissions.RepoArn(repository),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "actions_run_hook_output", r, repository, "", "")

	repo, err := c.Catalog.GetRepository(ctx, repository)
	if c.handleAPIError(ctx, w, r, err) {
		return
	}

	taskResult, err := c.Actions.GetTaskResult(ctx, repo.Name, runID, hookRunID)
	if c.handleAPIError(ctx, w, r, err) {
		return
	}

	if taskResult.StartTime.IsZero() { // skipped task
		writeResponse(w, r, http.StatusOK, nil)
		return
	}

	logPath := taskResult.LogPath()
	reader, err := c.BlockAdapter.Get(ctx, block.ObjectPointer{
		StorageNamespace: repo.StorageNamespace,
		IdentifierType:   block.IdentifierTypeRelative,
		Identifier:       logPath,
	}, -1)
	if c.handleAPIError(ctx, w, r, err) {
		return
	}
	defer func() {
		_ = reader.Close()
	}()

	cd := mime.FormatMediaType("attachment", map[string]string{"filename": filepath.Base(logPath)})
	w.Header().Set("Content-Disposition", cd)
	w.Header().Set("Content-Type", "application/octet-stream")
	_, err = io.Copy(w, reader)
	if err != nil {
		c.Logger.WithError(err).WithField("log_path", logPath).Warn("Write run hook output")
	}
}

func (c *Controller) ListBranches(w http.ResponseWriter, r *http.Request, repository string, params apigen.ListBranchesParams) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.ListBranchesAction,
			Resource: permissions.RepoArn(repository),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "list_branches", r, repository, "", "")

	res, hasMore, err := c.Catalog.ListBranches(ctx, repository, paginationPrefix(params.Prefix), paginationAmount(params.Amount), paginationAfter(params.After))
	if c.handleAPIError(ctx, w, r, err) {
		return
	}

	refs := make([]apigen.Ref, 0, len(res))
	for _, branch := range res {
		refs = append(refs, apigen.Ref{
			CommitId: branch.Reference,
			Id:       branch.Name,
		})
	}
	response := apigen.RefList{
		Results:    refs,
		Pagination: paginationFor(hasMore, refs, "Id"),
	}
	writeResponse(w, r, http.StatusOK, response)
}

func (c *Controller) CreateBranch(w http.ResponseWriter, r *http.Request, body apigen.CreateBranchJSONRequestBody, repository string) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.CreateBranchAction,
			Resource: permissions.BranchArn(repository, body.Name),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "create_branch", r, repository, body.Name, "")

	commitLog, err := c.Catalog.CreateBranch(ctx, repository, body.Name, body.Source, graveler.WithForce(swag.BoolValue(body.Force)))
	if c.handleAPIError(ctx, w, r, err) {
		return
	}
	w.WriteHeader(http.StatusCreated)
	_, _ = io.WriteString(w, commitLog.Reference)
}

func (c *Controller) DeleteBranch(w http.ResponseWriter, r *http.Request, repository, branch string, body apigen.DeleteBranchParams) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.DeleteBranchAction,
			Resource: permissions.BranchArn(repository, branch),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "delete_branch", r, repository, branch, "")

	err := c.Catalog.DeleteBranch(ctx, repository, branch, graveler.WithForce(swag.BoolValue(body.Force)))
	if c.handleAPIError(ctx, w, r, err) {
		return
	}
	writeResponse(w, r, http.StatusNoContent, nil)
}

func (c *Controller) GetBranch(w http.ResponseWriter, r *http.Request, repository, branch string) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.ReadBranchAction,
			Resource: permissions.BranchArn(repository, branch),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "get_branch", r, repository, branch, "")
	reference, err := c.Catalog.GetBranchReference(ctx, repository, branch)
	if c.handleAPIError(ctx, w, r, err) {
		return
	}
	response := apigen.Ref{
		CommitId: reference,
		Id:       branch,
	}
	writeResponse(w, r, http.StatusOK, response)
}

func (c *Controller) handleAPIErrorCallback(ctx context.Context, w http.ResponseWriter, r *http.Request, err error, cb func(w http.ResponseWriter, r *http.Request, code int, v interface{})) bool {
	// verify if request canceled even if there is no error, early exit point
	if httputil.IsRequestCanceled(r) {
		cb(w, r, httpStatusClientClosedRequest, httpStatusClientClosedRequestText)
		return true
	}
	if err == nil {
		return false
	}

	log := c.Logger.WithContext(ctx).WithError(err)

	// Handle Hook Errors
	var hookAbortErr *graveler.HookAbortError
	if errors.As(err, &hookAbortErr) {
		log.WithField("run_id", hookAbortErr.RunID).Warn("aborted by hooks")
		cb(w, r, http.StatusPreconditionFailed, err)
		return true
	}

	// order of case is important, more specific errors should be first
	switch {
	case errors.Is(err, graveler.ErrLinkAddressNotFound),
		errors.Is(err, graveler.ErrLinkAddressExpired):
		log.Debug("Expired or invalid address token")
		cb(w, r, http.StatusBadRequest, "bad address token (expired or invalid)")

	case errors.Is(err, graveler.ErrNotFound),
		errors.Is(err, actions.ErrNotFound),
		errors.Is(err, auth.ErrNotFound),
		errors.Is(err, kv.ErrNotFound):
		log.Debug("Not found")
		cb(w, r, http.StatusNotFound, err)

	case errors.Is(err, block.ErrForbidden),
		errors.Is(err, graveler.ErrProtectedBranch),
		errors.Is(err, graveler.ErrReadOnlyRepository):
		cb(w, r, http.StatusForbidden, err)

	case errors.Is(err, graveler.ErrDirtyBranch),
		errors.Is(err, graveler.ErrCommitMetaRangeDirtyBranch),
		errors.Is(err, graveler.ErrInvalidValue),
		errors.Is(err, validator.ErrInvalidValue),
		errors.Is(err, catalog.ErrPathRequiredValue),
		errors.Is(err, graveler.ErrNoChanges),
		errors.Is(err, permissions.ErrInvalidServiceName),
		errors.Is(err, permissions.ErrInvalidAction),
		errors.Is(err, model.ErrValidationError),
		errors.Is(err, graveler.ErrInvalidRef),
		errors.Is(err, actions.ErrParamConflict),
		errors.Is(err, graveler.ErrDereferenceCommitWithStaging),
		errors.Is(err, graveler.ErrParentOutOfRange),
		errors.Is(err, graveler.ErrCherryPickMergeNoParent),
		errors.Is(err, graveler.ErrInvalidMergeStrategy),
		errors.Is(err, block.ErrInvalidAddress),
		errors.Is(err, block.ErrOperationNotSupported):
		log.Debug("Bad request")
		cb(w, r, http.StatusBadRequest, err)

	case errors.Is(err, graveler.ErrNotUnique),
		errors.Is(err, graveler.ErrConflictFound),
		errors.Is(err, graveler.ErrRevertMergeNoParent):
		log.Debug("Conflict")
		cb(w, r, http.StatusConflict, err)

	case errors.Is(err, graveler.ErrLockNotAcquired):
		log.Debug("Lock not acquired")
		cb(w, r, http.StatusInternalServerError, "branch is currently locked, try again later")

	case errors.Is(err, block.ErrDataNotFound):
		log.Debug("No data")
		cb(w, r, http.StatusGone, "No data")

	case errors.Is(err, auth.ErrAlreadyExists):
		log.Debug("Already exists")
		cb(w, r, http.StatusConflict, "Already exists")

	case errors.Is(err, graveler.ErrTooManyTries):
		log.Debug("Retried too many times")
		cb(w, r, http.StatusLocked, "Too many attempts, try again later")
	case errors.Is(err, graveler.ErrPreconditionFailed):
		log.Debug("Precondition failed")
		cb(w, r, http.StatusPreconditionFailed, "Precondition failed")
	case errors.Is(err, authentication.ErrNotImplemented):
		cb(w, r, http.StatusNotImplemented, "Not implemented")
	case errors.Is(err, authentication.ErrInvalidSTS):
		cb(w, r, http.StatusUnauthorized, http.StatusText(http.StatusUnauthorized))
	case err != nil:
		c.Logger.WithContext(ctx).WithError(err).Error("API call returned status internal server error")
		cb(w, r, http.StatusInternalServerError, err)

	default:
		return false
	}

	return true
}

func (c *Controller) handleAPIError(ctx context.Context, w http.ResponseWriter, r *http.Request, err error) bool {
	return c.handleAPIErrorCallback(ctx, w, r, err, writeError)
}

func (c *Controller) ResetBranch(w http.ResponseWriter, r *http.Request, body apigen.ResetBranchJSONRequestBody, repository, branch string) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.RevertBranchAction,
			Resource: permissions.BranchArn(repository, branch),
		},
	}) {
		return
	}
	ctx := r.Context()

	c.LogAction(ctx, "reset_branch", r, repository, branch, "")

	var err error

	switch body.Type {
	case entryTypeCommonPrefix:
		err = c.Catalog.ResetEntries(ctx, repository, branch, swag.StringValue(body.Path), graveler.WithForce(swag.BoolValue(body.Force)))
	case "reset":
		err = c.Catalog.ResetBranch(ctx, repository, branch, graveler.WithForce(swag.BoolValue(body.Force)))
	case entryTypeObject:
		err = c.Catalog.ResetEntry(ctx, repository, branch, swag.StringValue(body.Path), graveler.WithForce(swag.BoolValue(body.Force)))
	default:
		writeError(w, r, http.StatusBadRequest, "unknown reset type")
		return
	}

	if c.handleAPIError(ctx, w, r, err) {
		return
	}
	writeResponse(w, r, http.StatusNoContent, nil)
}

func (c *Controller) HardResetBranch(w http.ResponseWriter, r *http.Request, repository, branch string, params apigen.HardResetBranchParams) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			// TODO(ozkatz): Can we have another action here?
			Action:   permissions.RevertBranchAction,
			Resource: permissions.BranchArn(repository, branch),
		},
	}) {
		return
	}
	ctx := r.Context()

	c.LogAction(ctx, "hard_reset_branch", r, repository, branch, "")

	err := c.Catalog.HardResetBranch(ctx, repository, branch, params.Ref, graveler.WithForce(swag.BoolValue(params.Force)))
	if c.handleAPIError(ctx, w, r, err) {
		return
	}
	writeResponse(w, r, http.StatusNoContent, nil)
}

func (c *Controller) ImportStart(w http.ResponseWriter, r *http.Request, body apigen.ImportStartJSONRequestBody, repository, branch string) {
	perm := permissions.Node{
		Type: permissions.NodeTypeAnd,
		Nodes: []permissions.Node{
			{
				Permission: permissions.Permission{
					Action:   permissions.WriteObjectAction,
					Resource: permissions.BranchArn(repository, branch),
				},
			},
			{
				Permission: permissions.Permission{
					Action:   permissions.CreateCommitAction,
					Resource: permissions.BranchArn(repository, branch),
				},
			},
		},
	}
	// Add import permissions per source
	// Add object permissions per destination
	for _, source := range body.Paths {
		perm.Nodes = append(perm.Nodes,
			permissions.Node{Permission: permissions.Permission{
				Action:   permissions.ImportFromStorageAction,
				Resource: permissions.StorageNamespace(source.Path),
			}},
			permissions.Node{Permission: permissions.Permission{
				Action:   permissions.WriteObjectAction,
				Resource: permissions.ObjectArn(repository, source.Destination),
			}})
	}
	if !c.authorize(w, r, perm) {
		return
	}

	ctx := r.Context()
	c.LogAction(ctx, "import", r, repository, branch, "")

	user, err := auth.GetUser(ctx)
	if err != nil {
		writeError(w, r, http.StatusUnauthorized, "missing user")
		return
	}
	metadata := map[string]string{}
	if body.Commit.Metadata != nil {
		metadata = body.Commit.Metadata.AdditionalProperties
	}
	paths := make([]catalog.ImportPath, 0, len(body.Paths))
	for _, p := range body.Paths {
		pathType, err := catalog.GetImportPathType(p.Type)
		if c.handleAPIError(ctx, w, r, err) {
			return
		}
		paths = append(paths, catalog.ImportPath{
			Destination: p.Destination,
			Path:        p.Path,
			Type:        pathType,
		})
	}

	importID, err := c.Catalog.Import(r.Context(), repository, branch, catalog.ImportRequest{
		Paths: paths,
		Commit: catalog.ImportCommit{
			CommitMessage: body.Commit.Message,
			Committer:     user.Committer(),
			Metadata:      metadata,
		},
		Force: swag.BoolValue(body.Force),
	})
	if c.handleAPIError(ctx, w, r, err) {
		return
	}
	writeResponse(w, r, http.StatusAccepted, apigen.ImportCreationResponse{
		Id: importID,
	})
}

func importStatusToResponse(status *graveler.ImportStatus) apigen.ImportStatus {
	resp := apigen.ImportStatus{
		Completed:       status.Completed,
		IngestedObjects: &status.Progress,
		UpdateTime:      status.UpdatedAt,
	}

	if status.Error != nil {
		resp.Error = &apigen.Error{Message: status.Error.Error()}
	}
	if status.MetaRangeID != "" {
		metarange := status.MetaRangeID.String()
		resp.MetarangeId = &metarange
	}

	commitLog := catalog.CommitRecordToLog(status.Commit)
	if commitLog != nil {
		resp.Commit = &apigen.Commit{
			Committer:    commitLog.Committer,
			CreationDate: commitLog.CreationDate.Unix(),
			Id:           commitLog.Reference,
			Message:      commitLog.Message,
			MetaRangeId:  commitLog.MetaRangeID,
			Metadata:     &apigen.Commit_Metadata{AdditionalProperties: commitLog.Metadata},
			Parents:      commitLog.Parents,
			Version:      apiutil.Ptr(int(commitLog.Version)),
			Generation:   apiutil.Ptr(int64(commitLog.Generation)),
		}
	}

	return resp
}

func (c *Controller) ImportStatus(w http.ResponseWriter, r *http.Request, repository, branch string, params apigen.ImportStatusParams) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.ReadBranchAction,
			Resource: permissions.BranchArn(repository, branch),
		},
	}) {
		return
	}
	ctx := r.Context()
	status, err := c.Catalog.GetImportStatus(ctx, repository, params.Id)
	if c.handleAPIError(ctx, w, r, err) {
		return
	}

	resp := importStatusToResponse(status)
	writeResponse(w, r, http.StatusOK, resp)
}

func (c *Controller) ImportCancel(w http.ResponseWriter, r *http.Request, repository, branch string, params apigen.ImportCancelParams) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.ImportCancelAction,
			Resource: permissions.BranchArn(repository, branch),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "cancel_import", r, repository, branch, "")
	err := c.Catalog.CancelImport(ctx, repository, params.Id)
	if c.handleAPIError(ctx, w, r, err) {
		return
	}

	writeResponse(w, r, http.StatusNoContent, nil)
}

func (c *Controller) Commit(w http.ResponseWriter, r *http.Request, body apigen.CommitJSONRequestBody, repository, branch string, params apigen.CommitParams) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.CreateCommitAction,
			Resource: permissions.BranchArn(repository, branch),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "create_commit", r, repository, branch, "")
	user, err := auth.GetUser(ctx)
	if err != nil {
		writeError(w, r, http.StatusUnauthorized, "missing user")
		return
	}
	var metadata map[string]string
	if body.Metadata != nil {
		metadata = body.Metadata.AdditionalProperties
	}

	newCommit, err := c.Catalog.Commit(ctx, repository, branch, body.Message, user.Committer(), metadata, body.Date, params.SourceMetarange, swag.BoolValue(body.AllowEmpty), graveler.WithForce(swag.BoolValue(body.Force)))
	if c.handleAPIError(ctx, w, r, err) {
		return
	}
	commitResponse(w, r, newCommit)
}

func (c *Controller) CreateCommitRecord(w http.ResponseWriter, r *http.Request, body apigen.CreateCommitRecordJSONRequestBody, repository string) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.CreateCommitAction,
			Resource: permissions.RepoArn(repository),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "create_commit_record", r, repository, body.CommitId, "")
	_, err := auth.GetUser(ctx)
	if err != nil {
		writeError(w, r, http.StatusUnauthorized, "missing user")
		return
	}
	err = c.Catalog.CreateCommitRecord(ctx, repository, body.CommitId, body.Version, body.Committer, body.Message, body.MetarangeId, body.CreationDate, body.Parents, body.Metadata.AdditionalProperties, int(body.Generation), graveler.WithForce(swag.BoolValue(body.Force)))
	if c.handleAPIError(ctx, w, r, err) {
		return
	}

	writeResponse(w, r, http.StatusNoContent, nil)
}

func commitResponse(w http.ResponseWriter, r *http.Request, newCommit *catalog.CommitLog) {
	response := apigen.Commit{
		Committer:    newCommit.Committer,
		CreationDate: newCommit.CreationDate.Unix(),
		Id:           newCommit.Reference,
		Message:      newCommit.Message,
		MetaRangeId:  newCommit.MetaRangeID,
		Metadata:     &apigen.Commit_Metadata{AdditionalProperties: newCommit.Metadata},
		Parents:      newCommit.Parents,
		Version:      apiutil.Ptr(int(newCommit.Version)),
		Generation:   apiutil.Ptr(int64(newCommit.Generation)),
	}
	writeResponse(w, r, http.StatusCreated, response)
}

func (c *Controller) DiffBranch(w http.ResponseWriter, r *http.Request, repository, branch string, params apigen.DiffBranchParams) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.ListObjectsAction,
			Resource: permissions.RepoArn(repository),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "diff_workspace", r, repository, branch, "")

	diff, hasMore, err := c.Catalog.DiffUncommitted(
		ctx,
		repository,
		branch,
		paginationPrefix(params.Prefix),
		paginationDelimiter(params.Delimiter),
		paginationAmount(params.Amount),
		paginationAfter(params.After),
	)
	if c.handleAPIError(ctx, w, r, err) {
		return
	}

	results := make([]apigen.Diff, 0, len(diff))
	for _, d := range diff {
		pathType := entryTypeObject
		if d.CommonLevel {
			pathType = entryTypeCommonPrefix
		}
		diff := apigen.Diff{
			Path:     d.Path,
			Type:     transformDifferenceTypeToString(d.Type),
			PathType: pathType,
		}
		if !d.CommonLevel {
			diff.SizeBytes = swag.Int64(d.Size)
		}
		results = append(results, diff)
	}
	response := apigen.DiffList{
		Pagination: paginationFor(hasMore, results, "Path"),
		Results:    results,
	}
	writeResponse(w, r, http.StatusOK, response)
}

func (c *Controller) DeleteObject(w http.ResponseWriter, r *http.Request, repository, branch string, params apigen.DeleteObjectParams) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.DeleteObjectAction,
			Resource: permissions.ObjectArn(repository, params.Path),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "delete_object", r, repository, branch, "")
	err := c.Catalog.DeleteEntry(ctx, repository, branch, params.Path, graveler.WithForce(swag.BoolValue(params.Force)))
	if c.handleAPIError(ctx, w, r, err) {
		return
	}
	writeResponse(w, r, http.StatusNoContent, nil)
}

func (c *Controller) UploadObjectPreflight(w http.ResponseWriter, r *http.Request, repository, branch string, params apigen.UploadObjectPreflightParams) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.WriteObjectAction,
			Resource: permissions.ObjectArn(repository, params.Path),
		},
	}) {
		return
	}

	ctx := r.Context()
	c.LogAction(ctx, "put_object_preflight", r, repository, branch, "")

	writeResponse(w, r, http.StatusNoContent, nil)
}

func (c *Controller) UploadObject(w http.ResponseWriter, r *http.Request, repository, branch string, params apigen.UploadObjectParams) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.WriteObjectAction,
			Resource: permissions.ObjectArn(repository, params.Path),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "put_object", r, repository, branch, "")

	repo, err := c.Catalog.GetRepository(ctx, repository)
	if c.handleAPIError(ctx, w, r, err) {
		return
	}

	// check if branch exists - it is still a possibility, but we don't want to upload large object when the branch was not there in the first place
	branchExists, err := c.Catalog.BranchExists(ctx, repository, branch)
	if c.handleAPIError(ctx, w, r, err) {
		return
	}
	if !branchExists {
		writeError(w, r, http.StatusNotFound, fmt.Sprintf("branch '%s' not found", branch))
		return
	}

	// before writing body, ensure preconditions - this means we essentially check for object existence twice:
	// once before uploading the body to save resources and time,
	//	and then graveler will check again when passed a SetOptions.
	allowOverwrite := true
	if params.IfNoneMatch != nil {
		if swag.StringValue((*string)(params.IfNoneMatch)) != "*" {
			writeError(w, r, http.StatusBadRequest, "Unsupported value for If-None-Match - Only \"*\" is supported")
			return
		}
		// check if exists
		_, err := c.Catalog.GetEntry(ctx, repo.Name, branch, params.Path, catalog.GetEntryParams{})
		if err == nil {
			writeError(w, r, http.StatusPreconditionFailed, "path already exists")
			return
		}
		if !errors.Is(err, graveler.ErrNotFound) {
			writeError(w, r, http.StatusInternalServerError, err)
			return
		}
		allowOverwrite = false
	}

	// read request body parse multipart for "content" and upload the data
	contentType := catalog.ContentTypeOrDefault(r.Header.Get("Content-Type"))
	mediaType, p, err := mime.ParseMediaType(contentType)
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}

	var blob *upload.Blob
	if mediaType != "multipart/form-data" {
		// handle non-multipart, direct content upload
		address := c.PathProvider.NewPath()
		blob, err = upload.WriteBlob(ctx, c.BlockAdapter, repo.StorageNamespace, address, r.Body, r.ContentLength,
			block.PutOpts{StorageClass: params.StorageClass})
		if err != nil {
			writeError(w, r, http.StatusInternalServerError, err)
			return
		}
	} else {
		// handle multipart upload
		boundary, ok := p["boundary"]
		if !ok {
			writeError(w, r, http.StatusInternalServerError, http.ErrMissingBoundary)
			return
		}

		contentUploaded := false
		reader := multipart.NewReader(r.Body, boundary)
		for !contentUploaded {
			part, err := reader.NextPart()
			if err == io.EOF {
				break
			}
			if err != nil {
				writeError(w, r, http.StatusInternalServerError, err)
				return
			}
			contentType = part.Header.Get("Content-Type")
			partName := part.FormName()
			if partName == "content" {
				// upload the first "content" and exit the loop
				address := c.PathProvider.NewPath()
				blob, err = upload.WriteBlob(ctx, c.BlockAdapter, repo.StorageNamespace, address, part, -1, block.PutOpts{StorageClass: params.StorageClass})
				if err != nil {
					_ = part.Close()
					writeError(w, r, http.StatusInternalServerError, err)
					return
				}
				contentUploaded = true
			}
			_ = part.Close()
		}
		if !contentUploaded {
			err := fmt.Errorf("multipart upload missing key 'content': %w", http.ErrMissingFile)
			writeError(w, r, http.StatusInternalServerError, err)
			return
		}
	}
	// write metadata
	writeTime := time.Now()
	entryBuilder := catalog.NewDBEntryBuilder().
		Path(params.Path).
		PhysicalAddress(blob.PhysicalAddress).
		CreationDate(writeTime).
		Size(blob.Size).
		Checksum(blob.Checksum).
		ContentType(contentType)
	if blob.RelativePath {
		entryBuilder.AddressType(catalog.AddressTypeRelative)
	} else {
		entryBuilder.AddressType(catalog.AddressTypeFull)
	}
	meta := extractLakeFSMetadata(r.Header)
	if len(meta) > 0 {
		entryBuilder.Metadata(meta)
	}
	entry := entryBuilder.Build()

	err = c.Catalog.CreateEntry(ctx, repo.Name, branch, entry, graveler.WithIfAbsent(!allowOverwrite), graveler.WithForce(swag.BoolValue(params.Force)))
	if errors.Is(err, graveler.ErrPreconditionFailed) {
		writeError(w, r, http.StatusPreconditionFailed, "path already exists")
		return
	}
	if c.handleAPIError(ctx, w, r, err) {
		return
	}

	identifierType := block.IdentifierTypeFull
	if blob.RelativePath {
		identifierType = block.IdentifierTypeRelative
	}

	qk, err := c.BlockAdapter.ResolveNamespace(repo.StorageNamespace, blob.PhysicalAddress, identifierType)
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}

	response := apigen.ObjectStats{
		Checksum:        blob.Checksum,
		Mtime:           writeTime.Unix(),
		Path:            params.Path,
		PathType:        entryTypeObject,
		PhysicalAddress: qk.Format(),
		SizeBytes:       swag.Int64(blob.Size),
		ContentType:     &contentType,
		Metadata:        &apigen.ObjectUserMetadata{AdditionalProperties: meta},
	}
	writeResponse(w, r, http.StatusCreated, response)
}

func (c *Controller) StageObject(w http.ResponseWriter, r *http.Request, body apigen.StageObjectJSONRequestBody, repository, branch string, params apigen.StageObjectParams) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.WriteObjectAction,
			Resource: permissions.ObjectArn(repository, params.Path),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "stage_object", r, repository, branch, "")

	repo, err := c.Catalog.GetRepository(ctx, repository)
	if c.handleAPIError(ctx, w, r, err) {
		return
	}
	// write metadata
	qk, err := c.BlockAdapter.ResolveNamespace(repo.StorageNamespace, body.PhysicalAddress, block.IdentifierTypeFull)
	if c.handleAPIError(ctx, w, r, err) {
		return
	}

	// see what storage type this is and whether it fits our configuration
	uriRegex := c.BlockAdapter.GetStorageNamespaceInfo().ValidityRegex
	if match, err := regexp.MatchString(uriRegex, body.PhysicalAddress); err != nil || !match {
		writeError(w, r, http.StatusBadRequest, fmt.Sprintf("physical address is not valid for block adapter: %s",
			c.BlockAdapter.BlockstoreType(),
		))
		return
	}

	// take mtime from request, if any
	writeTime := time.Now()
	if body.Mtime != nil {
		writeTime = time.Unix(*body.Mtime, 0)
	}

	physicalAddress, addressType := normalizePhysicalAddress(repo.StorageNamespace, body.PhysicalAddress)

	entryBuilder := catalog.NewDBEntryBuilder().
		CommonLevel(false).
		Path(params.Path).
		PhysicalAddress(physicalAddress).
		AddressType(addressType).
		CreationDate(writeTime).
		Size(body.SizeBytes).
		Checksum(body.Checksum).
		ContentType(swag.StringValue(body.ContentType))
	if body.Metadata != nil {
		entryBuilder.Metadata(body.Metadata.AdditionalProperties)
	}
	entry := entryBuilder.Build()

	err = c.Catalog.CreateEntry(ctx, repo.Name, branch, entry, graveler.WithForce(swag.BoolValue(body.Force)))
	if c.handleAPIError(ctx, w, r, err) {
		return
	}
	response := apigen.ObjectStats{
		Checksum:        entry.Checksum,
		Mtime:           entry.CreationDate.Unix(),
		Path:            entry.Path,
		PathType:        entryTypeObject,
		PhysicalAddress: qk.Format(),
		SizeBytes:       swag.Int64(entry.Size),
		ContentType:     swag.String(entry.ContentType),
	}
	writeResponse(w, r, http.StatusCreated, response)
}

func (c *Controller) CopyObject(w http.ResponseWriter, r *http.Request, body apigen.CopyObjectJSONRequestBody, repository, branch string, params apigen.CopyObjectParams) {
	srcPath := body.SrcPath
	destPath := params.DestPath
	if !c.authorize(w, r, permissions.Node{
		Type: permissions.NodeTypeAnd,
		Nodes: []permissions.Node{
			{
				Permission: permissions.Permission{
					Action:   permissions.ReadObjectAction,
					Resource: permissions.ObjectArn(repository, srcPath),
				},
			},
			{
				Permission: permissions.Permission{
					Action:   permissions.WriteObjectAction,
					Resource: permissions.ObjectArn(repository, destPath),
				},
			},
		},
	}) {
		return
	}

	ctx := r.Context()
	c.LogAction(ctx, "copy_object", r, repository, branch, destPath)

	repo, err := c.Catalog.GetRepository(ctx, repository)
	if c.handleAPIError(ctx, w, r, err) {
		return
	}

	// verify destination is a branch (and exists)
	branchExists, err := c.Catalog.BranchExists(ctx, repository, branch)
	if c.handleAPIError(ctx, w, r, err) {
		return
	}
	if !branchExists {
		writeError(w, r, http.StatusNotFound, fmt.Sprintf("branch '%s' not found", branch))
		return
	}

	// use destination branch as source if not specified
	srcRef := swag.StringValue(body.SrcRef)
	if srcRef == "" {
		srcRef = branch
	}

	// copy entry
	entry, err := c.Catalog.CopyEntry(ctx, repository, srcRef, srcPath, repository, branch, destPath, graveler.WithForce(swag.BoolValue(body.Force)))
	if c.handleAPIError(ctx, w, r, err) {
		return
	}

	qk, err := c.BlockAdapter.ResolveNamespace(repo.StorageNamespace, entry.PhysicalAddress, block.IdentifierTypeRelative)
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}

	var metadata map[string]string
	if entry.Metadata != nil {
		metadata = entry.Metadata
	} else {
		metadata = map[string]string{}
	}

	response := apigen.ObjectStats{
		Checksum:        entry.Checksum,
		Mtime:           entry.CreationDate.Unix(),
		Path:            entry.Path,
		PathType:        entryTypeObject,
		PhysicalAddress: qk.Format(),
		SizeBytes:       swag.Int64(entry.Size),
		ContentType:     swag.String(entry.ContentType),
		Metadata:        &apigen.ObjectUserMetadata{AdditionalProperties: metadata},
	}
	writeResponse(w, r, http.StatusCreated, response)
}

func (c *Controller) RevertBranch(w http.ResponseWriter, r *http.Request, body apigen.RevertBranchJSONRequestBody, repository, branch string) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.RevertBranchAction,
			Resource: permissions.BranchArn(repository, branch),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "revert_branch", r, repository, branch, "")

	user, err := auth.GetUser(ctx)
	if err != nil {
		writeError(w, r, http.StatusUnauthorized, "user not found")
		return
	}
	err = c.Catalog.Revert(ctx, repository, branch, catalog.RevertParams{
		Reference:    body.Ref,
		Committer:    user.Committer(),
		ParentNumber: body.ParentNumber,
		AllowEmpty:   swag.BoolValue(body.AllowEmpty),
	}, graveler.WithForce(swag.BoolValue(body.Force)))
	if c.handleAPIError(ctx, w, r, err) {
		return
	}
	writeResponse(w, r, http.StatusNoContent, nil)
}

func (c *Controller) CherryPick(w http.ResponseWriter, r *http.Request, body apigen.CherryPickJSONRequestBody, repository string, branch string) {
	if !c.authorize(w, r, permissions.Node{
		Type: permissions.NodeTypeAnd,
		Nodes: []permissions.Node{
			{
				Permission: permissions.Permission{
					Action:   permissions.CreateCommitAction,
					Resource: permissions.BranchArn(repository, branch),
				},
			},
			{
				Permission: permissions.Permission{
					Action:   permissions.ReadCommitAction,
					Resource: permissions.RepoArn(repository),
				},
			},
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "cherry_pick", r, repository, branch, body.Ref)

	user, err := auth.GetUser(ctx)
	if err != nil {
		writeError(w, r, http.StatusUnauthorized, "user not found")
		return
	}
	newCommit, err := c.Catalog.CherryPick(ctx, repository, branch, catalog.CherryPickParams{
		Reference:    body.Ref,
		Committer:    user.Committer(),
		ParentNumber: body.ParentNumber,
	}, graveler.WithForce(swag.BoolValue(body.Force)))
	if c.handleAPIError(ctx, w, r, err) {
		return
	}

	commitResponse(w, r, newCommit)
}

func (c *Controller) GetCommit(w http.ResponseWriter, r *http.Request, repository, commitID string) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.ReadCommitAction,
			Resource: permissions.RepoArn(repository),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "get_commit", r, repository, commitID, "")
	commit, err := c.Catalog.GetCommit(ctx, repository, commitID)
	if errors.Is(err, graveler.ErrRepositoryNotFound) {
		writeError(w, r, http.StatusNotFound, "repository not found")
		return
	}
	if errors.Is(err, graveler.ErrNotFound) {
		writeError(w, r, http.StatusNotFound, "commit not found")
		return
	}
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}
	response := apigen.Commit{
		Id:           commit.Reference,
		Committer:    commit.Committer,
		CreationDate: commit.CreationDate.Unix(),
		Message:      commit.Message,
		MetaRangeId:  commit.MetaRangeID,
		Metadata:     &apigen.Commit_Metadata{AdditionalProperties: commit.Metadata},
		Parents:      commit.Parents,
		Generation:   apiutil.Ptr(int64(commit.Generation)),
		Version:      apiutil.Ptr(int(commit.Version)),
	}
	writeResponse(w, r, http.StatusOK, response)
}

func (c *Controller) InternalGetGarbageCollectionRules(w http.ResponseWriter, r *http.Request, repository string) {
	c.GetGCRules(w, r, repository)
}

func (c *Controller) SetGarbageCollectionRulesPreflight(w http.ResponseWriter, r *http.Request, repository string) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.SetGarbageCollectionRulesAction,
			Resource: permissions.RepoArn(repository),
		},
	}) {
		return
	}

	ctx := r.Context()
	c.LogAction(ctx, "set_gc_collection_rules_preflight", r, repository, "", "")

	writeResponse(w, r, http.StatusNoContent, nil)
}

func (c *Controller) InternalSetGarbageCollectionRules(w http.ResponseWriter, r *http.Request, body apigen.InternalSetGarbageCollectionRulesJSONRequestBody, repository string) {
	c.SetGCRules(w, r, apigen.SetGCRulesJSONRequestBody(body), repository)
}

func (c *Controller) InternalDeleteGarbageCollectionRules(w http.ResponseWriter, r *http.Request, repository string) {
	c.DeleteGCRules(w, r, repository)
}

func (c *Controller) PrepareGarbageCollectionCommits(w http.ResponseWriter, r *http.Request, repository string) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.PrepareGarbageCollectionCommitsAction,
			Resource: permissions.RepoArn(repository),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "prepare_garbage_collection_commits", r, repository, "", "")
	gcRunMetadata, err := c.Catalog.PrepareExpiredCommits(ctx, repository)
	if c.handleAPIError(ctx, w, r, err) {
		return
	}
	presignedURL, _, err := c.BlockAdapter.GetPreSignedURL(ctx, block.ObjectPointer{
		Identifier:     gcRunMetadata.CommitsCSVLocation,
		IdentifierType: block.IdentifierTypeFull,
	}, block.PreSignModeRead)
	if err != nil {
		c.Logger.WithError(err).Warn("Failed to presign url for GC commits")
	}
	writeResponse(w, r, http.StatusCreated, apigen.GarbageCollectionPrepareResponse{
		GcCommitsLocation:     gcRunMetadata.CommitsCSVLocation,
		GcAddressesLocation:   gcRunMetadata.AddressLocation,
		RunId:                 gcRunMetadata.RunID,
		GcCommitsPresignedUrl: &presignedURL,
	})
}

func (c *Controller) InternalGetBranchProtectionRules(w http.ResponseWriter, r *http.Request, repository string) {
	c.GetBranchProtectionRules(w, r, repository)
}

func (c *Controller) InternalDeleteBranchProtectionRule(w http.ResponseWriter, r *http.Request, body apigen.InternalDeleteBranchProtectionRuleJSONRequestBody, repository string) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.SetBranchProtectionRulesAction,
			Resource: permissions.RepoArn(repository),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "delete_branch_protection_rule", r, repository, "", "")

	rules, _, err := c.Catalog.GetBranchProtectionRules(ctx, repository)
	if c.handleAPIError(ctx, w, r, err) {
		return
	}
	for p := range rules.BranchPatternToBlockedActions {
		if p == body.Pattern {
			delete(rules.BranchPatternToBlockedActions, p)
			err = c.Catalog.SetBranchProtectionRules(ctx, repository, rules, nil)
			if c.handleAPIError(ctx, w, r, err) {
				return
			}
			writeResponse(w, r, http.StatusNoContent, nil)
			return
		}
	}
	writeResponse(w, r, http.StatusNotFound, nil)
}

func (c *Controller) CreateBranchProtectionRulePreflight(w http.ResponseWriter, r *http.Request, repository string) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.SetBranchProtectionRulesAction,
			Resource: permissions.RepoArn(repository),
		},
	}) {
		return
	}

	ctx := r.Context()
	c.LogAction(ctx, "create_branch_protection_rule_preflight", r, repository, "", "")

	writeResponse(w, r, http.StatusNoContent, nil)
}

func (c *Controller) InternalCreateBranchProtectionRule(w http.ResponseWriter, r *http.Request, body apigen.InternalCreateBranchProtectionRuleJSONRequestBody, repository string) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.SetBranchProtectionRulesAction,
			Resource: permissions.RepoArn(repository),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "create_branch_protection_rule", r, repository, "", "")
	rules, _, err := c.Catalog.GetBranchProtectionRules(ctx, repository)
	if !errors.Is(err, graveler.ErrNotFound) {
		if c.handleAPIError(ctx, w, r, err) {
			return
		}
	}
	if rules == nil {
		rules = &graveler.BranchProtectionRules{}
	}
	if rules.BranchPatternToBlockedActions == nil {
		rules.BranchPatternToBlockedActions = make(map[string]*graveler.BranchProtectionBlockedActions)
	}
	blockedActions := &graveler.BranchProtectionBlockedActions{
		Value: []graveler.BranchProtectionBlockedAction{graveler.BranchProtectionBlockedAction_STAGING_WRITE, graveler.BranchProtectionBlockedAction_COMMIT},
	}
	rules.BranchPatternToBlockedActions[body.Pattern] = blockedActions
	err = c.Catalog.SetBranchProtectionRules(ctx, repository, rules, nil)
	if c.handleAPIError(ctx, w, r, err) {
		return
	}
	writeResponse(w, r, http.StatusNoContent, nil)
}

func (c *Controller) GetMetaRange(w http.ResponseWriter, r *http.Request, repository, metaRange string) {
	if !c.authorize(w, r, permissions.Node{
		Type: permissions.NodeTypeAnd,
		Nodes: []permissions.Node{
			{
				Permission: permissions.Permission{
					Action:   permissions.ListObjectsAction,
					Resource: permissions.RepoArn(repository),
				},
			},
			{
				Permission: permissions.Permission{
					Action:   permissions.ReadRepositoryAction,
					Resource: permissions.RepoArn(repository),
				},
			},
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "metadata_get_metarange", r, repository, "", "")

	metarange, err := c.Catalog.GetMetaRange(ctx, repository, metaRange)
	if c.handleAPIError(ctx, w, r, err) {
		return
	}

	response := apigen.StorageURI{
		Location: string(metarange),
	}
	w.Header().Set("Location", string(metarange))
	writeResponse(w, r, http.StatusOK, response)
}

func (c *Controller) GetRange(w http.ResponseWriter, r *http.Request, repository, pRange string) {
	if !c.authorize(w, r, permissions.Node{
		Type: permissions.NodeTypeAnd,
		Nodes: []permissions.Node{
			{
				Permission: permissions.Permission{
					Action:   permissions.ListObjectsAction,
					Resource: permissions.RepoArn(repository),
				},
			},
			{
				Permission: permissions.Permission{
					Action:   permissions.ReadRepositoryAction,
					Resource: permissions.RepoArn(repository),
				},
			},
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "metadata_get_range", r, repository, "", "")

	rng, err := c.Catalog.GetRange(ctx, repository, pRange)
	if c.handleAPIError(ctx, w, r, err) {
		return
	}
	response := apigen.StorageURI{
		Location: string(rng),
	}
	w.Header().Set("Location", string(rng))
	writeResponse(w, r, http.StatusOK, response)
}

func (c *Controller) DumpRefs(w http.ResponseWriter, r *http.Request, repository string) {
	if !c.authorize(w, r, permissions.Node{
		Type: permissions.NodeTypeAnd,
		Nodes: []permissions.Node{
			{
				Permission: permissions.Permission{
					Action:   permissions.ListTagsAction,
					Resource: permissions.RepoArn(repository),
				},
			},
			{
				Permission: permissions.Permission{
					Action:   permissions.ListBranchesAction,
					Resource: permissions.RepoArn(repository),
				},
			},
			{
				Permission: permissions.Permission{
					Action:   permissions.ListCommitsAction,
					Resource: permissions.RepoArn(repository),
				},
			},
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "dump_repository_refs", r, repository, "", "")

	repo, err := c.Catalog.GetRepository(ctx, repository)
	if c.handleAPIError(ctx, w, r, err) {
		return
	}

	// dump all types:
	tagsID, err := c.Catalog.DumpTags(ctx, repository)
	if c.handleAPIError(ctx, w, r, err) {
		return
	}

	branchesID, err := c.Catalog.DumpBranches(ctx, repository)
	if c.handleAPIError(ctx, w, r, err) {
		return
	}
	commitsID, err := c.Catalog.DumpCommits(ctx, repository)
	if c.handleAPIError(ctx, w, r, err) {
		return
	}

	response := apigen.RefsDump{
		BranchesMetaRangeId: branchesID,
		CommitsMetaRangeId:  commitsID,
		TagsMetaRangeId:     tagsID,
	}

	// write this to the block store
	manifestBytes, err := json.MarshalIndent(response, "", "  ")
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}
	err = c.BlockAdapter.Put(ctx, block.ObjectPointer{
		StorageNamespace: repo.StorageNamespace,
		IdentifierType:   block.IdentifierTypeRelative,
		Identifier:       fmt.Sprintf("%s/refs_manifest.json", c.Config.Committed.BlockStoragePrefix),
	}, int64(len(manifestBytes)), bytes.NewReader(manifestBytes), block.PutOpts{})
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}
	writeResponse(w, r, http.StatusCreated, response)
}

func (c *Controller) RestoreRefs(w http.ResponseWriter, r *http.Request, body apigen.RestoreRefsJSONRequestBody, repository string) {
	if !c.authorize(w, r, permissions.Node{
		Type: permissions.NodeTypeAnd,
		Nodes: []permissions.Node{
			{
				Permission: permissions.Permission{
					Action:   permissions.CreateTagAction,
					Resource: permissions.RepoArn(repository),
				},
			},
			{
				Permission: permissions.Permission{
					Action:   permissions.CreateBranchAction,
					Resource: permissions.RepoArn(repository),
				},
			},
			{
				Permission: permissions.Permission{
					Action:   permissions.CreateCommitAction,
					Resource: permissions.RepoArn(repository),
				},
			},
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "restore_repository_refs", r, repository, "", "")

	repo, err := c.Catalog.GetRepository(ctx, repository)
	if c.handleAPIError(ctx, w, r, err) {
		return
	}

	// ensure no refs currently found
	_, _, err = c.Catalog.ListCommits(ctx, repo.Name, repo.DefaultBranch, catalog.LogParams{
		PathList:      make([]catalog.PathRecord, 0),
		FromReference: "",
		Amount:        1,
	})
	if !errors.Is(err, graveler.ErrNotFound) {
		writeError(w, r, http.StatusBadRequest, "can only restore into a bare repository")
		return
	}

	// load commits
	err = c.Catalog.LoadCommits(ctx, repo.Name, body.CommitsMetaRangeId, graveler.WithForce(swag.BoolValue(body.Force)))
	if c.handleAPIError(ctx, w, r, err) {
		return
	}

	err = c.Catalog.LoadBranches(ctx, repo.Name, body.BranchesMetaRangeId, graveler.WithForce(swag.BoolValue(body.Force)))
	if c.handleAPIError(ctx, w, r, err) {
		return
	}

	err = c.Catalog.LoadTags(ctx, repo.Name, body.TagsMetaRangeId, graveler.WithForce(swag.BoolValue(body.Force)))
	if c.handleAPIError(ctx, w, r, err) {
		return
	}
}

func (c *Controller) DumpSubmit(w http.ResponseWriter, r *http.Request, repository string) {
	if !c.authorize(w, r, permissions.Node{
		Type: permissions.NodeTypeAnd,
		Nodes: []permissions.Node{
			{
				Permission: permissions.Permission{
					Action:   permissions.ListTagsAction,
					Resource: permissions.RepoArn(repository),
				},
			},
			{
				Permission: permissions.Permission{
					Action:   permissions.ListBranchesAction,
					Resource: permissions.RepoArn(repository),
				},
			},
			{
				Permission: permissions.Permission{
					Action:   permissions.ListCommitsAction,
					Resource: permissions.RepoArn(repository),
				},
			},
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "dump_repository", r, repository, "", "")

	taskID, err := c.Catalog.DumpRepositorySubmit(ctx, repository)
	if c.handleAPIError(ctx, w, r, err) {
		return
	}

	writeResponse(w, r, http.StatusAccepted, apigen.TaskInfo{
		Id: taskID,
	})
}

func (c *Controller) DumpStatus(w http.ResponseWriter, r *http.Request, repository string, params apigen.DumpStatusParams) {
	if !c.authorize(w, r, permissions.Node{
		Type: permissions.NodeTypeAnd,
		Nodes: []permissions.Node{
			{
				Permission: permissions.Permission{
					Action:   permissions.ListTagsAction,
					Resource: permissions.RepoArn(repository),
				},
			},
			{
				Permission: permissions.Permission{
					Action:   permissions.ListBranchesAction,
					Resource: permissions.RepoArn(repository),
				},
			},
			{
				Permission: permissions.Permission{
					Action:   permissions.ListCommitsAction,
					Resource: permissions.RepoArn(repository),
				},
			},
		},
	}) {
		return
	}

	// get the current status
	ctx := r.Context()
	status, err := c.Catalog.DumpRepositoryStatus(ctx, repository, params.TaskId)
	if c.handleAPIError(ctx, w, r, err) {
		return
	}

	// build response based on status
	response := &apigen.RepositoryDumpStatus{
		Id:         params.TaskId,
		Done:       status.Task.Done,
		UpdateTime: status.Task.UpdatedAt.AsTime(),
	}
	if status.Task.Error != "" {
		response.Error = apiutil.Ptr(status.Task.Error)
	}
	if status.Task.Done && status.Info != nil {
		response.Refs = &apigen.RefsDump{
			CommitsMetaRangeId:  status.Info.CommitsMetarangeId,
			TagsMetaRangeId:     status.Info.TagsMetarangeId,
			BranchesMetaRangeId: status.Info.BranchesMetarangeId,
		}
	}
	writeResponse(w, r, http.StatusOK, response)
}

func (c *Controller) RestoreSubmit(w http.ResponseWriter, r *http.Request, body apigen.RestoreSubmitJSONRequestBody, repository string) {
	if !c.authorize(w, r, permissions.Node{
		Type: permissions.NodeTypeAnd,
		Nodes: []permissions.Node{
			{
				Permission: permissions.Permission{
					Action:   permissions.CreateTagAction,
					Resource: permissions.RepoArn(repository),
				},
			},
			{
				Permission: permissions.Permission{
					Action:   permissions.CreateBranchAction,
					Resource: permissions.RepoArn(repository),
				},
			},
			{
				Permission: permissions.Permission{
					Action:   permissions.CreateCommitAction,
					Resource: permissions.RepoArn(repository),
				},
			},
		},
	}) {
		return
	}

	ctx := r.Context()
	c.LogAction(ctx, "restore_repository", r, repository, "", "")

	info := &catalog.RepositoryDumpInfo{
		CommitsMetarangeId:  body.CommitsMetaRangeId,
		TagsMetarangeId:     body.TagsMetaRangeId,
		BranchesMetarangeId: body.BranchesMetaRangeId,
	}
	taskID, err := c.Catalog.RestoreRepositorySubmit(ctx, repository, info, graveler.WithForce(swag.BoolValue(body.Force)))
	if errors.Is(err, catalog.ErrNonEmptyRepository) {
		writeError(w, r, http.StatusBadRequest, "can only restore into a bare repository")
		return
	}
	if c.handleAPIError(ctx, w, r, err) {
		return
	}
	writeResponse(w, r, http.StatusAccepted, apigen.TaskInfo{
		Id: taskID,
	})
}

func (c *Controller) RestoreStatus(w http.ResponseWriter, r *http.Request, repository string, params apigen.RestoreStatusParams) {
	if !c.authorize(w, r, permissions.Node{
		Type: permissions.NodeTypeAnd,
		Nodes: []permissions.Node{
			{
				Permission: permissions.Permission{
					Action:   permissions.CreateTagAction,
					Resource: permissions.RepoArn(repository),
				},
			},
			{
				Permission: permissions.Permission{
					Action:   permissions.CreateBranchAction,
					Resource: permissions.RepoArn(repository),
				},
			},
			{
				Permission: permissions.Permission{
					Action:   permissions.CreateCommitAction,
					Resource: permissions.RepoArn(repository),
				},
			},
		},
	}) {
		return
	}

	// get the current status
	ctx := r.Context()
	status, err := c.Catalog.RestoreRepositoryStatus(ctx, repository, params.TaskId)
	if c.handleAPIError(ctx, w, r, err) {
		return
	}

	// build response based on status
	response := &apigen.RepositoryRestoreStatus{
		Id:         params.TaskId,
		Done:       status.Task.Done,
		UpdateTime: status.Task.UpdatedAt.AsTime(),
	}
	if status.Task.Error != "" {
		response.Error = apiutil.Ptr(status.Task.Error)
	}
	writeResponse(w, r, http.StatusOK, response)
}

func (c *Controller) CreateSymlinkFile(w http.ResponseWriter, r *http.Request, repository, branch string, params apigen.CreateSymlinkFileParams) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.WriteObjectAction,
			Resource: permissions.ObjectArn(repository, branch),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "create_symlink", r, repository, branch, "")

	// read repo
	repo, err := c.Catalog.GetRepository(ctx, repository)
	if c.handleAPIError(ctx, w, r, err) {
		return
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
			repository,
			branch,
			swag.StringValue(params.Location),
			after,
			"",
			-1)
		if c.handleAPIError(ctx, w, r, err) {
			return
		}
		// loop all entries enter to map[path] physicalAddress
		for _, entry := range entries {
			qk, err := c.BlockAdapter.ResolveNamespace(repo.StorageNamespace, entry.PhysicalAddress, entry.AddressType.ToIdentifierType())
			if err != nil {
				writeError(w, r, http.StatusInternalServerError, fmt.Sprintf("error while resolving address: %s", err))
				return
			}
			idx := strings.LastIndex(entry.Path, "/")
			var path string
			if idx != -1 {
				path = entry.Path[0:idx]
			}
			if path != currentPath {
				// push current
				err := writeSymlink(ctx, repo, branch, path, currentAddresses, c.BlockAdapter)
				if err != nil {
					writeError(w, r, http.StatusInternalServerError, fmt.Sprintf("error while writing symlinks: %s", err))
					return
				}
				currentPath = path
				currentAddresses = []string{qk.Format()}
			} else {
				currentAddresses = append(currentAddresses, qk.Format())
			}
		}
		after = entries[len(entries)-1].Path
	}
	if len(currentAddresses) > 0 {
		err = writeSymlink(ctx, repo, branch, currentPath, currentAddresses, c.BlockAdapter)
		if err != nil {
			writeError(w, r, http.StatusInternalServerError, fmt.Sprintf("error while writing symlinks: %s", err))
			return
		}
	}
	metaLocation := fmt.Sprintf("%s/%s", repo.StorageNamespace, lakeFSPrefix)
	response := apigen.StorageURI{
		Location: metaLocation,
	}
	writeResponse(w, r, http.StatusCreated, response)
}

func writeSymlink(ctx context.Context, repo *catalog.Repository, branch, path string, addresses []string, adapter block.Adapter) error {
	address := fmt.Sprintf("%s/%s/%s/%s/symlink.txt", lakeFSPrefix, repo.Name, branch, path)
	data := strings.Join(addresses, "\n")
	err := adapter.Put(ctx, block.ObjectPointer{
		StorageNamespace: repo.StorageNamespace,
		IdentifierType:   block.IdentifierTypeRelative,
		Identifier:       address,
	}, int64(len(data)), strings.NewReader(data), block.PutOpts{})
	return err
}

func (c *Controller) DiffRefs(w http.ResponseWriter, r *http.Request, repository, leftRef, rightRef string, params apigen.DiffRefsParams) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.ListObjectsAction,
			Resource: permissions.RepoArn(repository),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "diff_refs", r, repository, rightRef, leftRef)
	diffFunc := c.Catalog.Compare // default diff type is three-dot
	if params.Type != nil && *params.Type == "two_dot" {
		diffFunc = c.Catalog.Diff
	}

	diff, hasMore, err := diffFunc(ctx, repository, leftRef, rightRef, catalog.DiffParams{
		Limit:            paginationAmount(params.Amount),
		After:            paginationAfter(params.After),
		Prefix:           paginationPrefix(params.Prefix),
		Delimiter:        paginationDelimiter(params.Delimiter),
		AdditionalFields: nil,
	})
	if c.handleAPIError(ctx, w, r, err) {
		return
	}
	results := make([]apigen.Diff, 0, len(diff))
	for _, d := range diff {
		pathType := entryTypeObject
		if d.CommonLevel {
			pathType = entryTypeCommonPrefix
		}
		diff := apigen.Diff{
			Path:     d.Path,
			Type:     transformDifferenceTypeToString(d.Type),
			PathType: pathType,
		}
		if !d.CommonLevel {
			diff.SizeBytes = swag.Int64(d.Size)
		}
		results = append(results, diff)
	}
	response := apigen.DiffList{
		Pagination: paginationFor(hasMore, results, "Path"),
		Results:    results,
	}
	writeResponse(w, r, http.StatusOK, response)
}

func (c *Controller) LogCommits(w http.ResponseWriter, r *http.Request, repository, ref string, params apigen.LogCommitsParams) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.ReadBranchAction,
			Resource: permissions.BranchArn(repository, ref),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "get_branch_commit_log", r, repository, ref, "")

	// get commit log
	commitLog, hasMore, err := c.Catalog.ListCommits(ctx, repository, ref, catalog.LogParams{
		PathList:      resolvePathList(params.Objects, params.Prefixes),
		FromReference: paginationAfter(params.After),
		Amount:        paginationAmount(params.Amount),
		Limit:         swag.BoolValue(params.Limit),
		FirstParent:   swag.BoolValue(params.FirstParent),
		Since:         params.Since,
		StopAt:        swag.StringValue(params.StopAt),
	})
	if c.handleAPIError(ctx, w, r, err) {
		return
	}

	serializedCommits := make([]apigen.Commit, 0, len(commitLog))
	for _, commit := range commitLog {
		metadata := apigen.Commit_Metadata{
			AdditionalProperties: commit.Metadata,
		}
		serializedCommits = append(serializedCommits, apigen.Commit{
			Committer:    commit.Committer,
			CreationDate: commit.CreationDate.Unix(),
			Id:           commit.Reference,
			Message:      commit.Message,
			Metadata:     &metadata,
			MetaRangeId:  commit.MetaRangeID,
			Parents:      commit.Parents,
			Generation:   apiutil.Ptr(int64(commit.Generation)),
			Version:      apiutil.Ptr(int(commit.Version)),
		})
	}

	response := apigen.CommitList{
		Pagination: paginationFor(hasMore, serializedCommits, "Id"),
		Results:    serializedCommits,
	}
	writeResponse(w, r, http.StatusOK, response)
}

func (c *Controller) HeadObject(w http.ResponseWriter, r *http.Request, repository, ref string, params apigen.HeadObjectParams) {
	if !c.authorizeCallback(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.ReadObjectAction,
			Resource: permissions.ObjectArn(repository, params.Path),
		},
	}, func(w http.ResponseWriter, r *http.Request, code int, v interface{}) {
		writeResponse(w, r, code, nil)
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "head_object", r, repository, ref, "")

	// read the FS entry
	entry, err := c.Catalog.GetEntry(ctx, repository, ref, params.Path, catalog.GetEntryParams{})
	if err != nil {
		c.handleAPIErrorCallback(ctx, w, r, err, func(w http.ResponseWriter, r *http.Request, code int, v interface{}) {
			writeResponse(w, r, code, nil)
		})
		return
	}
	if entry.Expired {
		writeResponse(w, r, http.StatusGone, nil)
		return
	}

	etag := httputil.ETag(entry.Checksum)
	w.Header().Set("ETag", etag)
	lastModified := httputil.HeaderTimestamp(entry.CreationDate)
	w.Header().Set("Last-Modified", lastModified)
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Type", entry.ContentType)
	// for security, make sure the browser and any proxies en route don't cache the response
	w.Header().Set("Cache-Control", "no-store, must-revalidate")
	w.Header().Set("Expires", "0")

	// calculate possible byte range, if any.
	if params.Range != nil {
		rng, err := httputil.ParseRange(*params.Range, entry.Size)
		if err != nil {
			writeResponse(w, r, http.StatusRequestedRangeNotSatisfiable, nil)
			return
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", rng.StartOffset, rng.EndOffset, entry.Size))
		w.Header().Set("Content-Length", fmt.Sprintf("%d", rng.EndOffset-rng.StartOffset+1))
		writeResponse(w, r, http.StatusPartialContent, nil)
	} else {
		w.Header().Set("Content-Length", fmt.Sprint(entry.Size))
	}
}

func (c *Controller) GetObject(w http.ResponseWriter, r *http.Request, repository, ref string, params apigen.GetObjectParams) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.ReadObjectAction,
			Resource: permissions.ObjectArn(repository, params.Path),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "get_object", r, repository, ref, "")

	repo, err := c.Catalog.GetRepository(ctx, repository)
	if c.handleAPIError(ctx, w, r, err) {
		return
	}

	// read the FS entry
	entry, err := c.Catalog.GetEntry(ctx, repository, ref, params.Path, catalog.GetEntryParams{})
	if c.handleAPIError(ctx, w, r, err) {
		return
	}
	c.Logger.Tracef("get repo %s ref %s path %s: %+v", repository, ref, params.Path, entry)
	if entry.Expired {
		writeError(w, r, http.StatusGone, "resource expired")
		return
	}

	eTag := httputil.ETag(entry.Checksum)

	// check ETag if not modified in request
	// Not using swag.StringValue to catch edge case where eTag == ""
	if params.IfNoneMatch != nil && *params.IfNoneMatch == eTag {
		w.WriteHeader(http.StatusNotModified)
		return
	}

	// if pre-sign, return a redirect
	pointer := block.ObjectPointer{
		StorageNamespace: repo.StorageNamespace,
		IdentifierType:   entry.AddressType.ToIdentifierType(),
		Identifier:       entry.PhysicalAddress,
	}
	if swag.BoolValue(params.Presign) {
		location, _, err := c.BlockAdapter.GetPreSignedURL(ctx, pointer, block.PreSignModeRead)
		if c.handleAPIError(ctx, w, r, err) {
			return
		}
		w.Header().Set("Location", location)
		w.WriteHeader(http.StatusFound)
		return
	}

	// set response headers
	w.Header().Set("ETag", eTag)
	lastModified := httputil.HeaderTimestamp(entry.CreationDate)
	w.Header().Set("Last-Modified", lastModified)
	w.Header().Set("Content-Type", entry.ContentType)
	// for security, make sure the browser and any proxies en route don't cache the response
	w.Header().Set("Cache-Control", "no-store, must-revalidate")
	w.Header().Set("Expires", "0")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("X-Frame-Options", "SAMEORIGIN")
	w.Header().Set("Content-Security-Policy", "default-src 'none'")

	// handle partial response if byte range supplied
	var reader io.ReadCloser
	if params.Range != nil {
		rng, err := httputil.ParseRange(*params.Range, entry.Size)
		if err != nil {
			writeError(w, r, http.StatusRequestedRangeNotSatisfiable, "Requested Range Not Satisfiable")
			return
		}
		reader, err = c.BlockAdapter.GetRange(ctx, pointer, rng.StartOffset, rng.EndOffset)
		if c.handleAPIError(ctx, w, r, err) {
			return
		}
		defer func() {
			_ = reader.Close()
		}()
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", rng.StartOffset, rng.EndOffset, entry.Size))
		w.Header().Set("Content-Length", fmt.Sprintf("%d", rng.EndOffset-rng.StartOffset+1))
		w.WriteHeader(http.StatusPartialContent)
	} else {
		reader, err = c.BlockAdapter.Get(ctx, pointer, entry.Size)
		if c.handleAPIError(ctx, w, r, err) {
			return
		}
		defer func() {
			_ = reader.Close()
		}()
		w.Header().Set("Content-Length", fmt.Sprint(entry.Size))
	}

	// copy the content
	_, err = io.Copy(w, reader)
	if err != nil {
		c.Logger.
			WithError(err).
			WithFields(logging.Fields{
				"storage_namespace": repo.StorageNamespace,
				"physical_address":  entry.PhysicalAddress,
			}).
			Debug("GetObject copy content")
	}
}

func (c *Controller) ListObjects(w http.ResponseWriter, r *http.Request, repository, ref string, params apigen.ListObjectsParams) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.ListObjectsAction,
			Resource: permissions.RepoArn(repository),
		},
	}) {
		return
	}
	ctx := r.Context()
	user, _ := auth.GetUser(ctx)
	c.LogAction(ctx, "list_objects", r, repository, ref, "")

	repo, err := c.Catalog.GetRepository(ctx, repository)
	if c.handleAPIError(ctx, w, r, err) {
		return
	}

	res, hasMore, err := c.Catalog.ListEntries(
		ctx,
		repository,
		ref,
		paginationPrefix(params.Prefix),
		paginationAfter(params.After),
		paginationDelimiter(params.Delimiter),
		paginationAmount(params.Amount),
	)
	if c.handleAPIError(ctx, w, r, err) {
		return
	}

	objList := make([]apigen.ObjectStats, 0, len(res))
	for _, entry := range res {
		qk, err := c.BlockAdapter.ResolveNamespace(repo.StorageNamespace, entry.PhysicalAddress, entry.AddressType.ToIdentifierType())
		if err != nil {
			writeError(w, r, http.StatusInternalServerError, err)
			return
		}

		if entry.CommonLevel {
			objList = append(objList, apigen.ObjectStats{
				Path:     entry.Path,
				PathType: entryTypeCommonPrefix,
			})
		} else {
			var mtime int64
			if !entry.CreationDate.IsZero() {
				mtime = entry.CreationDate.Unix()
			}
			objStat := apigen.ObjectStats{
				Checksum:        entry.Checksum,
				Mtime:           mtime,
				Path:            entry.Path,
				PhysicalAddress: qk.Format(),
				PathType:        entryTypeObject,
				SizeBytes:       swag.Int64(entry.Size),
				ContentType:     swag.String(entry.ContentType),
			}
			if (params.UserMetadata == nil || *params.UserMetadata) && entry.Metadata != nil {
				objStat.Metadata = &apigen.ObjectUserMetadata{AdditionalProperties: entry.Metadata}
			}
			if swag.BoolValue(params.Presign) {
				// check if the user has read permissions for this object
				authResponse, err := c.Auth.Authorize(ctx, &auth.AuthorizationRequest{
					Username: user.Username,
					RequiredPermissions: permissions.Node{
						Permission: permissions.Permission{
							Action:   permissions.ReadObjectAction,
							Resource: permissions.ObjectArn(repository, entry.Path),
						},
					},
				})
				if c.handleAPIError(ctx, w, r, err) {
					return
				}
				if authResponse.Allowed {
					var expiry time.Time
					objStat.PhysicalAddress, expiry, err = c.BlockAdapter.GetPreSignedURL(ctx, block.ObjectPointer{
						StorageNamespace: repo.StorageNamespace,
						IdentifierType:   entry.AddressType.ToIdentifierType(),
						Identifier:       entry.PhysicalAddress,
					}, block.PreSignModeRead)
					if c.handleAPIError(ctx, w, r, err) {
						return
					}
					if !expiry.IsZero() {
						objStat.PhysicalAddressExpiry = swag.Int64(expiry.Unix())
					}
				}
			}
			objList = append(objList, objStat)
		}
	}
	response := apigen.ObjectStatsList{
		Pagination: apigen.Pagination{
			HasMore:    hasMore,
			MaxPerPage: DefaultMaxPerPage,
			Results:    len(objList),
		},
		Results: objList,
	}
	if len(objList) > 0 && hasMore {
		lastObj := objList[len(objList)-1]
		response.Pagination.NextOffset = lastObj.Path
	}
	writeResponse(w, r, http.StatusOK, response)
}

func (c *Controller) StatObject(w http.ResponseWriter, r *http.Request, repository, ref string, params apigen.StatObjectParams) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.ReadObjectAction,
			Resource: permissions.ObjectArn(repository, params.Path),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "stat_object", r, repository, ref, "")

	repo, err := c.Catalog.GetRepository(ctx, repository)
	if c.handleAPIError(ctx, w, r, err) {
		return
	}

	entry, err := c.Catalog.GetEntry(ctx, repository, ref, params.Path, catalog.GetEntryParams{})
	if c.handleAPIError(ctx, w, r, err) {
		return
	}

	qk, err := c.BlockAdapter.ResolveNamespace(repo.StorageNamespace, entry.PhysicalAddress, entry.AddressType.ToIdentifierType())
	if c.handleAPIError(ctx, w, r, err) {
		return
	}

	objStat := apigen.ObjectStats{
		Checksum:        entry.Checksum,
		Mtime:           entry.CreationDate.Unix(),
		Path:            entry.Path,
		PathType:        entryTypeObject,
		PhysicalAddress: qk.Format(),
		SizeBytes:       swag.Int64(entry.Size),
		ContentType:     swag.String(entry.ContentType),
	}

	// add metadata if requested
	var metadata map[string]string
	if (params.UserMetadata == nil || *params.UserMetadata) && entry.Metadata != nil {
		metadata = entry.Metadata
	} else {
		metadata = map[string]string{}
	}
	objStat.Metadata = &apigen.ObjectUserMetadata{AdditionalProperties: metadata}

	code := http.StatusOK
	if entry.Expired {
		code = http.StatusGone
	} else if swag.BoolValue(params.Presign) {
		// need to pre-sign the physical address
		preSignedURL, expiry, err := c.BlockAdapter.GetPreSignedURL(ctx, block.ObjectPointer{
			StorageNamespace: repo.StorageNamespace,
			IdentifierType:   entry.AddressType.ToIdentifierType(),
			Identifier:       entry.PhysicalAddress,
		}, block.PreSignModeRead)
		if c.handleAPIError(ctx, w, r, err) {
			return
		}
		objStat.PhysicalAddress = preSignedURL
		if !expiry.IsZero() {
			objStat.PhysicalAddressExpiry = swag.Int64(expiry.Unix())
		}
	}
	writeResponse(w, r, code, objStat)
}

func (c *Controller) GetUnderlyingProperties(w http.ResponseWriter, r *http.Request, repository, ref string, params apigen.GetUnderlyingPropertiesParams) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.ReadObjectAction,
			Resource: permissions.ObjectArn(repository, params.Path),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "object_underlying_properties", r, repository, ref, "")

	// read repo
	repo, err := c.Catalog.GetRepository(ctx, repository)
	if c.handleAPIError(ctx, w, r, err) {
		return
	}

	entry, err := c.Catalog.GetEntry(ctx, repository, ref, params.Path, catalog.GetEntryParams{})
	if c.handleAPIError(ctx, w, r, err) {
		return
	}

	// read object properties from underlying storage
	properties, err := c.BlockAdapter.GetProperties(ctx, block.ObjectPointer{
		StorageNamespace: repo.StorageNamespace,
		IdentifierType:   entry.AddressType.ToIdentifierType(),
		Identifier:       entry.PhysicalAddress,
	})
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}

	response := apigen.UnderlyingObjectProperties{
		StorageClass: properties.StorageClass,
	}
	writeResponse(w, r, http.StatusOK, response)
}

func (c *Controller) MergeIntoBranch(w http.ResponseWriter, r *http.Request, body apigen.MergeIntoBranchJSONRequestBody, repository, sourceRef, destinationBranch string) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.CreateCommitAction,
			Resource: permissions.BranchArn(repository, destinationBranch),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "merge_branches", r, repository, destinationBranch, sourceRef)
	user, err := auth.GetUser(ctx)
	if err != nil {
		writeError(w, r, http.StatusUnauthorized, "user not found")
		return
	}
	metadata := map[string]string{}
	if body.Metadata != nil {
		metadata = body.Metadata.AdditionalProperties
	}

	reference, err := c.Catalog.Merge(ctx,
		repository, destinationBranch, sourceRef,
		user.Committer(),
		swag.StringValue(body.Message),
		metadata,
		swag.StringValue(body.Strategy),
		graveler.WithForce(swag.BoolValue(body.Force)))

	if errors.Is(err, graveler.ErrConflictFound) {
		writeResponse(w, r, http.StatusConflict, apigen.MergeResult{
			Reference: reference,
		})
		return
	}
	if c.handleAPIError(ctx, w, r, err) {
		return
	}
	writeResponse(w, r, http.StatusOK, apigen.MergeResult{
		Reference: reference,
	})
}

func (c *Controller) FindMergeBase(w http.ResponseWriter, r *http.Request, repository string, sourceRef string, destinationRef string) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.ListCommitsAction,
			Resource: permissions.RepoArn(repository),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "find_merge_base", r, repository, destinationRef, sourceRef)

	source, dest, base, err := c.Catalog.FindMergeBase(ctx, repository, destinationRef, sourceRef)
	if c.handleAPIError(ctx, w, r, err) {
		return
	}
	writeResponse(w, r, http.StatusOK, apigen.FindMergeBaseResult{
		BaseCommitId:        base,
		DestinationCommitId: dest,
		SourceCommitId:      source,
	})
}

func (c *Controller) ListTags(w http.ResponseWriter, r *http.Request, repository string, params apigen.ListTagsParams) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.ListTagsAction,
			Resource: permissions.RepoArn(repository),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "list_tags", r, repository, "", "")

	res, hasMore, err := c.Catalog.ListTags(ctx, repository, paginationPrefix(params.Prefix), paginationAmount(params.Amount), paginationAfter(params.After))
	if c.handleAPIError(ctx, w, r, err) {
		return
	}

	results := make([]apigen.Ref, 0, len(res))
	for _, tag := range res {
		results = append(results, apigen.Ref{
			CommitId: tag.CommitID,
			Id:       tag.ID,
		})
	}
	response := apigen.RefList{
		Results:    results,
		Pagination: paginationFor(hasMore, results, "Id"),
	}
	writeResponse(w, r, http.StatusOK, response)
}

func (c *Controller) CreateTag(w http.ResponseWriter, r *http.Request, body apigen.CreateTagJSONRequestBody, repository string) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.CreateTagAction,
			Resource: permissions.TagArn(repository, body.Id),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "create_tag", r, repository, body.Id, "")

	commitID, err := c.Catalog.CreateTag(ctx, repository, body.Id, body.Ref, graveler.WithForce(swag.BoolValue(body.Force)))
	if c.handleAPIError(ctx, w, r, err) {
		return
	}
	response := apigen.Ref{
		CommitId: commitID,
		Id:       body.Id,
	}
	writeResponse(w, r, http.StatusCreated, response)
}

func (c *Controller) DeleteTag(w http.ResponseWriter, r *http.Request, repository, tag string, params apigen.DeleteTagParams) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.DeleteTagAction,
			Resource: permissions.TagArn(repository, tag),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "delete_tag", r, repository, tag, "")
	err := c.Catalog.DeleteTag(ctx, repository, tag, graveler.WithForce(swag.BoolValue(params.Force)))
	if c.handleAPIError(ctx, w, r, err) {
		return
	}
	writeResponse(w, r, http.StatusNoContent, nil)
}

func (c *Controller) GetTag(w http.ResponseWriter, r *http.Request, repository, tag string) {
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.ReadTagAction,
			Resource: permissions.TagArn(repository, tag),
		},
	}) {
		return
	}
	ctx := r.Context()
	c.LogAction(ctx, "get_tag", r, repository, tag, "")
	reference, err := c.Catalog.GetTag(ctx, repository, tag)
	if c.handleAPIError(ctx, w, r, err) {
		return
	}
	response := apigen.Ref{
		CommitId: reference,
		Id:       tag,
	}
	writeResponse(w, r, http.StatusOK, response)
}

func newLoginConfig(c *config.Config) *apigen.LoginConfig {
	return &apigen.LoginConfig{
		RBAC:               &c.Auth.UIConfig.RBAC,
		LoginUrl:           c.Auth.UIConfig.LoginURL,
		LoginFailedMessage: &c.Auth.UIConfig.LoginFailedMessage,
		FallbackLoginUrl:   c.Auth.UIConfig.FallbackLoginURL,
		FallbackLoginLabel: c.Auth.UIConfig.FallbackLoginLabel,
		LoginCookieNames:   c.Auth.UIConfig.LoginCookieNames,
		LogoutUrl:          c.Auth.UIConfig.LogoutURL,
	}
}

func (c *Controller) GetSetupState(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// external auth reports as initialized to avoid triggering the setup wizard
	if c.Config.Auth.UIConfig.RBAC == config.AuthRBACExternal {
		response := apigen.SetupState{
			State:            swag.String(string(auth.SetupStateInitialized)),
			LoginConfig:      newLoginConfig(c.Config),
			CommPrefsMissing: swag.Bool(false),
		}
		writeResponse(w, r, http.StatusOK, response)
		return
	}

	savedState, err := c.MetadataManager.GetSetupState(ctx)
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}
	if savedState == auth.SetupStateNotInitialized {
		c.Collector.CollectEvent(stats.Event{Class: "global", Name: "preinit", Client: httputil.GetRequestLakeFSClient(r)})
	}

	response := apigen.SetupState{
		State:       swag.String(string(savedState)),
		LoginConfig: newLoginConfig(c.Config),
	}

	// if email subscription is disabled in the config, set the missing flag to false.
	// otherwise, check if the comm prefs are set.
	// if they are, set the missing flag to false.
	if !c.Config.EmailSubscription.Enabled {
		response.CommPrefsMissing = swag.Bool(false)
		writeResponse(w, r, http.StatusOK, response)
		return
	}

	prefsSet, err := c.MetadataManager.IsCommPrefsSet(ctx)
	switch {
	case errors.Is(err, auth.ErrNotFound):
		// comprefs may not be found for two reasons:
		// 1. The setup ran on an older version of lakeFS that didn't have commprefs. In this case, we treat it as set.
		// 2. The setup ran on a newer version of lakeFS that has commprefs, but the setup didn't complete. In this case, we treat it as not set.
		response.CommPrefsMissing = swag.Bool(savedState != auth.SetupStateInitialized)
	case err != nil:
		// failed to check if comm prefs are set, treating as set
		response.CommPrefsMissing = swag.Bool(false)
	default:
		response.CommPrefsMissing = swag.Bool(!prefsSet)
	}

	writeResponse(w, r, http.StatusOK, response)
}

func (c *Controller) Setup(w http.ResponseWriter, r *http.Request, body apigen.SetupJSONRequestBody) {
	if len(body.Username) == 0 {
		writeError(w, r, http.StatusBadRequest, "empty user display name")
		return
	}

	ctx := r.Context()
	initialized, err := c.MetadataManager.IsInitialized(ctx)
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}
	if initialized {
		writeError(w, r, http.StatusConflict, "lakeFS already initialized")
		return
	}

	// migrate the database if needed
	err = c.Migrator.Migrate(ctx)
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}

	if c.Config.Auth.UIConfig.RBAC == config.AuthRBACExternal {
		// nothing to do - users are managed elsewhere
		writeResponse(w, r, http.StatusOK, apigen.CredentialsWithSecret{})
		return
	}

	var cred *model.Credential
	if body.Key == nil {
		cred, err = setup.CreateInitialAdminUser(ctx, c.Auth, c.Config, c.MetadataManager, body.Username)
	} else {
		cred, err = setup.CreateInitialAdminUserWithKeys(ctx, c.Auth, c.Config, c.MetadataManager, body.Username, &body.Key.AccessKeyId, &body.Key.SecretAccessKey)
	}
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}

	meta := stats.NewMetadata(ctx, c.Logger, c.BlockAdapter.BlockstoreType(), c.MetadataManager, c.CloudMetadataProvider)
	c.Collector.SetInstallationID(meta.InstallationID)
	c.Collector.CollectMetadata(meta)
	c.Collector.CollectEvent(stats.Event{Class: "global", Name: "init", UserID: body.Username, Client: httputil.GetRequestLakeFSClient(r)})

	response := apigen.CredentialsWithSecret{
		AccessKeyId:     cred.AccessKeyID,
		SecretAccessKey: cred.SecretAccessKey,
		CreationDate:    cred.IssuedDate.Unix(),
	}
	writeResponse(w, r, http.StatusOK, response)
}

func (c *Controller) SetupCommPrefs(w http.ResponseWriter, r *http.Request, body apigen.SetupCommPrefsJSONRequestBody) {
	ctx := r.Context()
	emailAddress := swag.StringValue(body.Email)
	if emailAddress == "" {
		writeResponse(w, r, http.StatusBadRequest, "email is required")
		return
	}

	// validate email. if the user typed some value into the input, they might have a typo
	// we assume the intent was to provide a valid email, so we'll return an error
	if _, err := mail.ParseAddress(emailAddress); err != nil {
		c.Logger.WithError(err).WithField("email", emailAddress).Error("Setup comm prefs, invalid email address")
		writeError(w, r, http.StatusBadRequest, "invalid email address")
		return
	}

	// save comm prefs to metadata, for future in-app preferences/unsubscribe functionality
	commPrefs := auth.CommPrefs{
		UserEmail:       emailAddress,
		FeatureUpdates:  body.FeatureUpdates,
		SecurityUpdates: body.SecurityUpdates,
	}

	installationID, err := c.MetadataManager.UpdateCommPrefs(ctx, &commPrefs)
	if err != nil {
		c.Logger.WithError(err).WithField("email", emailAddress).Error("Setup comm prefs, failed to save comm prefs to metadata")
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}

	commPrefsED := stats.CommPrefs{
		Email:           commPrefs.UserEmail,
		InstallationID:  installationID,
		FeatureUpdates:  commPrefs.FeatureUpdates,
		SecurityUpdates: commPrefs.SecurityUpdates,
		BlockstoreType:  c.Config.BlockstoreType(),
	}
	// collect comm prefs
	go c.Collector.CollectCommPrefs(commPrefsED)

	writeResponse(w, r, http.StatusOK, nil)
}

func (c *Controller) GetCurrentUser(w http.ResponseWriter, r *http.Request) {
	u, err := auth.GetUser(r.Context())
	var user apigen.User
	if err == nil {
		user.Id = u.Username
		user.CreationDate = u.CreatedAt.Unix()
		if u.FriendlyName != nil {
			user.FriendlyName = u.FriendlyName
		} else {
			user.FriendlyName = &u.Username
		}
		user.Email = u.Email
	}
	response := apigen.CurrentUser{
		User: user,
	}
	writeResponse(w, r, http.StatusOK, response)
}

func (c *Controller) GetLakeFSVersion(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	_, err := auth.GetUser(ctx)
	if err != nil {
		writeError(w, r, http.StatusUnauthorized, ErrAuthenticatingRequest)
		return
	}
	writeResponse(w, r, http.StatusOK, c.getVersionConfig())
}

func (c *Controller) getVersionConfig() apigen.VersionConfig {
	// set upgrade recommended based on last security audit check
	var (
		upgradeRecommended *bool
		upgradeURL         *string
		latestVersion      *string
	)
	lastCheck, _ := c.AuditChecker.LastCheck()
	if lastCheck != nil {
		recommendedURL := lastCheck.UpgradeRecommendedURL()
		if recommendedURL != "" {
			upgradeRecommended = swag.Bool(true)
			upgradeURL = swag.String(recommendedURL)
		}
	}

	if c.Config.Security.CheckLatestVersion {
		latest, err := c.AuditChecker.CheckLatestVersion()
		// set upgrade recommended based on latest version
		if err != nil {
			c.Logger.WithError(err).Debug("failed to check latest version in releases")
		} else if latest != nil {
			latestVersion = swag.String(latest.LatestVersion)
			upgradeRecommended = swag.Bool(latest.Outdated || swag.BoolValue(upgradeRecommended))
			if latest.Outdated && upgradeURL == nil {
				upgradeURL = swag.String(version.DefaultReleasesURL)
			}
		}
	}

	return apigen.VersionConfig{
		UpgradeRecommended: upgradeRecommended,
		UpgradeUrl:         upgradeURL,
		Version:            swag.String(version.Version),
		LatestVersion:      latestVersion,
	}
}

func (c *Controller) GetGarbageCollectionConfig(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	_, err := auth.GetUser(ctx)
	if err != nil {
		writeError(w, r, http.StatusUnauthorized, ErrAuthenticatingRequest)
		return
	}

	writeResponse(w, r, http.StatusOK, apigen.GarbageCollectionConfig{
		GracePeriod: swag.Int(int(ref.LinkAddressTime.Seconds())),
	})
}

func (c *Controller) PostStatsEvents(w http.ResponseWriter, r *http.Request, body apigen.PostStatsEventsJSONRequestBody) {
	ctx := r.Context()
	user, err := auth.GetUser(ctx)
	if err != nil {
		writeError(w, r, http.StatusUnauthorized, ErrAuthenticatingRequest)
		return
	}

	for _, statsEv := range body.Events {
		if statsEv.Class == "" {
			writeError(w, r, http.StatusBadRequest, "invalid value: class is required")
			return
		}

		if statsEv.Name == "" {
			writeError(w, r, http.StatusBadRequest, "invalid value: name is required")
			return
		}

		if statsEv.Count < 0 {
			writeError(w, r, http.StatusBadRequest, "invalid value: count must be a non-negative integer")
			return
		}
	}

	client := httputil.GetRequestLakeFSClient(r)
	for _, statsEv := range body.Events {
		ev := stats.Event{
			Class:  statsEv.Class,
			Name:   statsEv.Name,
			UserID: user.Username,
			Client: client,
		}
		c.Collector.CollectEvents(ev, uint64(statsEv.Count))

		c.Logger.WithContext(ctx).WithFields(logging.Fields{
			"class":   ev.Class,
			"name":    ev.Name,
			"count":   statsEv.Count,
			"user_id": ev.UserID,
			"client":  ev.Client,
		}).Debug("sending stats events")
	}

	writeResponse(w, r, http.StatusNoContent, nil)
}

func writeError(w http.ResponseWriter, r *http.Request, code int, v interface{}) {
	apiErr := apigen.Error{
		Message: fmt.Sprint(v),
	}
	writeResponse(w, r, code, apiErr)
}

func writeResponse(w http.ResponseWriter, r *http.Request, code int, response interface{}) {
	// check first if the client canceled the request
	if httputil.IsRequestCanceled(r) {
		w.WriteHeader(httpStatusClientClosedRequest) // Client closed request
		return
	}
	// nobody - just status code
	if response == nil {
		w.WriteHeader(code)
		return
	}
	// encode response body as json
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(code)
	err := json.NewEncoder(w).Encode(response)
	if err != nil {
		logging.FromContext(r.Context()).WithError(err).WithField("code", code).Info("Failed to write encoded json response")
	}
}

func paginationAfter(v *apigen.PaginationAfter) string {
	if v == nil {
		return ""
	}
	return string(*v)
}

func paginationPrefix(v *apigen.PaginationPrefix) string {
	if v == nil {
		return ""
	}
	return string(*v)
}

func paginationDelimiter(v *apigen.PaginationDelimiter) string {
	if v == nil {
		return ""
	}
	return string(*v)
}

func paginationAmount(v *apigen.PaginationAmount) int {
	if v == nil {
		return DefaultPerPage
	}
	i := int(*v)
	if i > DefaultMaxPerPage {
		return DefaultMaxPerPage
	}
	if i <= 0 {
		return DefaultPerPage
	}
	return i
}

func resolvePathList(objects, prefixes *[]string) []catalog.PathRecord {
	var pathRecords []catalog.PathRecord
	if objects == nil && prefixes == nil {
		return make([]catalog.PathRecord, 0)
	}
	if objects != nil {
		for _, p := range *objects {
			if p != "" {
				pathRecords = append(pathRecords, catalog.PathRecord{
					Path:     catalog.Path(p),
					IsPrefix: false,
				})
			}
		}
	}
	if prefixes != nil {
		for _, p := range *prefixes {
			if p != "" {
				pathRecords = append(pathRecords, catalog.PathRecord{
					Path:     catalog.Path(p),
					IsPrefix: true,
				})
			}
		}
	}
	return pathRecords
}

func NewController(cfg *config.Config, catalog *catalog.Catalog, authenticator auth.Authenticator, authService auth.Service, authenticationService authentication.Service, blockAdapter block.Adapter, metadataManager auth.MetadataManager, migrator Migrator, collector stats.Collector, cloudMetadataProvider cloud.MetadataProvider, actions actionsHandler, auditChecker AuditChecker, logger logging.Logger, sessionStore sessions.Store, pathProvider upload.PathProvider, usageReporter stats.UsageReporterOperations) *Controller {
	return &Controller{
		Config:                cfg,
		Catalog:               catalog,
		Authenticator:         authenticator,
		Auth:                  authService,
		Authentication:        authenticationService,
		BlockAdapter:          blockAdapter,
		MetadataManager:       metadataManager,
		Migrator:              migrator,
		Collector:             collector,
		CloudMetadataProvider: cloudMetadataProvider,
		Actions:               actions,
		AuditChecker:          auditChecker,
		Logger:                logger,
		sessionStore:          sessionStore,
		PathProvider:          pathProvider,
		usageReporter:         usageReporter,
	}
}

func (c *Controller) LogAction(ctx context.Context, action string, r *http.Request, repository, ref, sourceRef string) {
	client := httputil.GetRequestLakeFSClient(r)
	ev := stats.Event{
		Class:      "api_server",
		Name:       action,
		Repository: repository,
		Ref:        ref,
		SourceRef:  sourceRef,
		Client:     client,
	}
	user, _ := auth.GetUser(ctx)
	if user != nil {
		ev.UserID = user.Username
	}

	sourceIP := httputil.SourceIP(r)

	c.Logger.WithContext(ctx).WithFields(logging.Fields{
		"class":      ev.Class,
		"name":       ev.Name,
		"repository": ev.Repository,
		"ref":        ev.Ref,
		"source_ref": ev.SourceRef,
		"user_id":    ev.UserID,
		"client":     ev.Client,
		"source_ip":  sourceIP,
	}).Debug("performing API action")
	c.Collector.CollectEvent(ev)
	usageCounter.Add(1)
}

func paginationFor(hasMore bool, results interface{}, fieldName string) apigen.Pagination {
	pagination := apigen.Pagination{
		HasMore:    hasMore,
		MaxPerPage: DefaultMaxPerPage,
	}
	if results == nil {
		return pagination
	}
	if reflect.TypeOf(results).Kind() != reflect.Slice {
		panic("results is not a slice")
	}
	s := reflect.ValueOf(results)
	pagination.Results = s.Len()
	if !hasMore || pagination.Results == 0 {
		return pagination
	}
	v := s.Index(pagination.Results - 1)
	token := v.FieldByName(fieldName)
	pagination.NextOffset = token.String()
	return pagination
}

func (c *Controller) authorizeCallback(w http.ResponseWriter, r *http.Request, perms permissions.Node, cb func(w http.ResponseWriter, r *http.Request, code int, v interface{})) bool {
	ctx := r.Context()
	user, err := auth.GetUser(ctx)
	if err != nil {
		cb(w, r, http.StatusUnauthorized, ErrAuthenticatingRequest)
		return false
	}
	resp, err := c.Auth.Authorize(ctx, &auth.AuthorizationRequest{
		Username:            user.Username,
		RequiredPermissions: perms,
	})
	if err != nil {
		cb(w, r, http.StatusInternalServerError, err)
		return false
	}
	if resp.Error != nil {
		cb(w, r, http.StatusUnauthorized, resp.Error)
		return false
	}
	if !resp.Allowed {
		cb(w, r, http.StatusInternalServerError, "User does not have the required permissions")
		return false
	}
	return true
}

func (c *Controller) authorize(w http.ResponseWriter, r *http.Request, perms permissions.Node) bool {
	return c.authorizeCallback(w, r, perms, writeError)
}

func (c *Controller) isNameValid(name, nameType string) (bool, string) {
	// URLs are % encoded. Allowing % signs in entity names would
	// limit the ability to use these entity names in the URL for both
	// client-side routing and API fetch requests
	if strings.Contains(name, "%") {
		return false, fmt.Sprintf("%s name cannot contain '%%'", nameType)
	}
	return true, ""
}

func decodeGCUncommittedMark(mark string) (*catalog.GCUncommittedMark, error) {
	if mark == "" {
		return nil, nil
	}
	data, err := base64.StdEncoding.DecodeString(mark)
	if err != nil {
		return nil, err
	}
	var result catalog.GCUncommittedMark
	err = json.Unmarshal(data, &result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

func encodeGCUncommittedMark(mark *catalog.GCUncommittedMark) (*string, error) {
	if mark == nil {
		return nil, nil
	}
	raw, err := json.Marshal(mark)
	if err != nil {
		return nil, err
	}

	token := base64.StdEncoding.EncodeToString(raw)
	return &token, nil
}

func extractLakeFSMetadata(header http.Header) map[string]string {
	meta := make(map[string]string)
	for k, v := range header {
		lowerKey := strings.ToLower(k)
		metaKey := ""
		switch {
		case strings.HasPrefix(lowerKey, apiutil.LakeFSHeaderMetadataPrefix):
			metaKey = lowerKey[len(apiutil.LakeFSHeaderMetadataPrefix):]
		case strings.HasPrefix(lowerKey, apiutil.LakeFSHeaderInternalPrefix):
			metaKey = apiutil.LakeFSMetadataPrefix + lowerKey[len(apiutil.LakeFSHeaderInternalPrefix):]
		default:
			continue
		}
		meta[metaKey] = v[0]
	}
	return meta
}

func (c *Controller) GetUsageReportSummary(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	installationID := c.usageReporter.InstallationID()
	if installationID == "" {
		writeError(w, r, http.StatusNotFound, "usage report is not enabled")
		return
	}

	// flush data before collecting usage - can help for single node deployments
	c.usageReporter.Flush(ctx)

	records, err := c.usageReporter.Records(ctx)
	if c.handleAPIError(ctx, w, r, err) {
		return
	}

	// base on content-type return plain text or json (default)
	if r.Header.Get("Accept") == "text/plain" {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		_, _ = fmt.Fprintf(w, "Usage for installation ID: %s\n", installationID)
		for _, rec := range records {
			_, _ = fmt.Fprintf(w, "%d-%02d: %12d\n", rec.Year, rec.Month, rec.Count)
		}
		return
	}

	// format response as json
	response := apigen.InstallationUsageReport{
		InstallationId: installationID,
		Reports:        make([]apigen.UsageReport, 0, len(records)),
	}
	for _, rec := range records {
		response.Reports = append(response.Reports, apigen.UsageReport{
			Count: rec.Count,
			Month: rec.Month,
			Year:  rec.Year,
		})
	}
	writeResponse(w, r, http.StatusOK, response)
}

func (c *Controller) CreateUserExternalPrincipal(w http.ResponseWriter, r *http.Request, body apigen.CreateUserExternalPrincipalJSONRequestBody, userID string, params apigen.CreateUserExternalPrincipalParams) {
	ctx := r.Context()
	if c.isExternalPrincipalNotSupported(ctx) {
		writeError(w, r, http.StatusNotImplemented, "Not implemented")
		return
	}
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.CreateUserExternalPrincipalAction,
			Resource: permissions.UserArn(userID),
		},
	}) {
		return
	}

	c.LogAction(ctx, "create_user_external_principal", r, "", "", "")

	err := c.Auth.CreateUserExternalPrincipal(ctx, userID, params.PrincipalId)
	if c.handleAPIError(ctx, w, r, err) {
		return
	}
	writeResponse(w, r, http.StatusCreated, nil)
}

func (c *Controller) DeleteUserExternalPrincipal(w http.ResponseWriter, r *http.Request, userID string, params apigen.DeleteUserExternalPrincipalParams) {
	ctx := r.Context()
	if c.isExternalPrincipalNotSupported(ctx) {
		writeError(w, r, http.StatusNotImplemented, "Not implemented")
		return
	}
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.DeleteUserExternalPrincipalAction,
			Resource: permissions.UserArn(userID),
		},
	}) {
		return
	}
	c.LogAction(ctx, "delete_user_external_principal", r, "", "", "")
	err := c.Auth.DeleteUserExternalPrincipal(ctx, userID, params.PrincipalId)
	if c.handleAPIError(ctx, w, r, err) {
		return
	}
	writeResponse(w, r, http.StatusNoContent, nil)
}

func (c *Controller) GetExternalPrincipal(w http.ResponseWriter, r *http.Request, params apigen.GetExternalPrincipalParams) {
	ctx := r.Context()
	if c.isExternalPrincipalNotSupported(ctx) {
		writeError(w, r, http.StatusNotImplemented, "Not implemented")
		return
	}
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.ReadExternalPrincipalAction,
			Resource: permissions.ExternalPrincipalArn(params.PrincipalId),
		},
	}) {
		return
	}
	c.LogAction(ctx, "get_external_principal", r, "", "", "")

	principal, err := c.Auth.GetExternalPrincipal(ctx, params.PrincipalId)
	if c.handleAPIError(ctx, w, r, err) {
		return
	}
	response := apigen.ExternalPrincipal{
		Id:     principal.ID,
		UserId: principal.UserID,
	}
	writeResponse(w, r, http.StatusOK, response)
}

func (c *Controller) ListUserExternalPrincipals(w http.ResponseWriter, r *http.Request, userID string, params apigen.ListUserExternalPrincipalsParams) {
	ctx := r.Context()
	if c.isExternalPrincipalNotSupported(ctx) {
		writeError(w, r, http.StatusNotImplemented, "Not implemented")
		return
	}
	if !c.authorize(w, r, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.ReadUserAction,
			Resource: permissions.UserArn(userID),
		},
	}) {
		return
	}

	c.LogAction(ctx, "list_user_external_principals", r, "", "", "")

	principals, paginator, err := c.Auth.ListUserExternalPrincipals(ctx, userID, &model.PaginationParams{
		Prefix: paginationPrefix(params.Prefix),
		Amount: paginationAmount(params.Amount),
		After:  paginationAfter(params.After),
	})

	if c.handleAPIError(ctx, w, r, err) {
		return
	}

	response := apigen.ExternalPrincipalList{
		Results: make([]apigen.ExternalPrincipal, len(principals)),
		Pagination: apigen.Pagination{
			HasMore:    paginator.NextPageToken != "",
			NextOffset: paginator.NextPageToken,
			Results:    paginator.Amount,
		},
	}

	for i, p := range principals {
		response.Results[i] = apigen.ExternalPrincipal{
			Id:     p.ID,
			UserId: p.UserID,
		}
	}
	writeResponse(w, r, http.StatusOK, response)
}

func (c *Controller) isExternalPrincipalNotSupported(ctx context.Context) bool {
	// if IsAuthUISimplified true then it means the user not using RBAC model
	return c.Config.IsAuthUISimplified() || !c.Auth.IsExternalPrincipalsEnabled(ctx)
}
