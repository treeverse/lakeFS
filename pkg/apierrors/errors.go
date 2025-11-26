package apierrors

import (
	"context"
	"errors"
	"net/http"

	"github.com/treeverse/lakefs/pkg/actions"
	"github.com/treeverse/lakefs/pkg/auth"
	authmodel "github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/authentication"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/catalogerrors"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/httputil"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/license"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/permissions"
	"github.com/treeverse/lakefs/pkg/validator"
)

// HandleAPIErrorCallback handles API errors with a custom callback function.
// This function is used by both synchronous and asynchronous operations to ensure
// consistent error classification and handling.
//
// For synchronous operations (HTTP requests):
//   - Pass w, r, and a callback that writes the error response
//
// For asynchronous operations (background tasks):
//   - Pass nil for w and r
//   - Use a callback that stores the error code and message in the task status
//
// The callback receives the status code and error message/value.
func HandleAPIErrorCallback(ctx context.Context, logger logging.Logger, w http.ResponseWriter, r *http.Request, err error, cb func(w http.ResponseWriter, r *http.Request, code int, v interface{})) bool {
	// verify if request canceled even if there is no error, early exit point
	// Only check if r is not nil (for sync operations)
	if r != nil && httputil.IsRequestCanceled(r) {
		cb(w, r, httputil.HttpStatusClientClosedRequest, httputil.HttpStatusClientClosedRequestText)
		return true
	}
	if err == nil {
		return false
	}

	log := logger.WithContext(ctx).WithError(err)

	// order of case is important, more specific errors should be first
	switch {
	case errors.Is(err, graveler.ErrLinkAddressInvalid),
		errors.Is(err, graveler.ErrLinkAddressExpired):
		log.Debug("Expired or invalid address token")
		cb(w, r, http.StatusBadRequest, err)

	case errors.Is(err, graveler.ErrNotFound),
		errors.Is(err, actions.ErrNotFound),
		errors.Is(err, auth.ErrNotFound),
		errors.Is(err, kv.ErrNotFound),
		errors.Is(err, config.ErrNoStorageConfig):
		log.Debug("Not found")
		cb(w, r, http.StatusNotFound, err)

	case errors.Is(err, block.ErrForbidden),
		errors.Is(err, graveler.ErrProtectedBranch),
		errors.Is(err, graveler.ErrReadOnlyRepository),
		errors.Is(err, graveler.ErrDeleteDefaultBranch):
		cb(w, r, http.StatusForbidden, err)

	case errors.Is(err, authentication.ErrSessionExpired):
		cb(w, r, http.StatusForbidden, "session expired")

	case errors.Is(err, authentication.ErrInvalidTokenFormat):
		cb(w, r, http.StatusUnauthorized, "invalid token format")

	case errors.Is(err, graveler.ErrDirtyBranch),
		errors.Is(err, graveler.ErrCommitMetaRangeDirtyBranch),
		errors.Is(err, graveler.ErrInvalidValue),
		errors.Is(err, validator.ErrInvalidValue),
		errors.Is(err, catalogerrors.ErrPathRequiredValue),
		errors.Is(err, graveler.ErrNoChanges),
		errors.Is(err, permissions.ErrInvalidServiceName),
		errors.Is(err, permissions.ErrInvalidAction),
		errors.Is(err, authmodel.ErrValidationError),
		errors.Is(err, graveler.ErrInvalidRef),
		errors.Is(err, actions.ErrParamConflict),
		errors.Is(err, graveler.ErrDereferenceCommitWithStaging),
		errors.Is(err, graveler.ErrParentOutOfRange),
		errors.Is(err, graveler.ErrCherryPickMergeNoParent),
		errors.Is(err, graveler.ErrInvalidMergeStrategy),
		errors.Is(err, block.ErrInvalidAddress),
		errors.Is(err, block.ErrOperationNotSupported),
		errors.Is(err, auth.ErrInvalidRequest),
		errors.Is(err, authentication.ErrInvalidRequest),
		errors.Is(err, graveler.ErrSameBranch),
		errors.Is(err, graveler.ErrInvalidPullRequestStatus),
		errors.Is(err, catalogerrors.ErrInvalidImportSource):
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

	case errors.Is(err, graveler.ErrRepositoryInDeletion):
		cb(w, r, http.StatusGone, err)

	case errors.Is(err, block.ErrDataNotFound):
		log.Debug("No data")
		cb(w, r, http.StatusGone, "No data")

	case errors.Is(err, auth.ErrAlreadyExists):
		log.Debug("Already exists")
		cb(w, r, http.StatusConflict, "Already exists")

	case errors.Is(err, graveler.ErrTooManyTries):
		log.Debug("Retried too many times")
		cb(w, r, http.StatusTooManyRequests, "Too many attempts, try again later")
	case errors.Is(err, kv.ErrSlowDown):
		log.Debug("KV Throttling")
		cb(w, r, http.StatusServiceUnavailable, "Throughput exceeded. Slow down and retry")
	case errors.Is(err, graveler.ErrPreconditionFailed):
		log.Debug("Precondition failed")
		cb(w, r, http.StatusPreconditionFailed, "Precondition failed")
	case errors.Is(err, authentication.ErrNotImplemented),
		errors.Is(err, auth.ErrNotImplemented),
		errors.Is(err, license.ErrNotImplemented),
		errors.Is(err, catalogerrors.ErrNotImplemented):
		cb(w, r, http.StatusNotImplemented, "Not implemented")
	case errors.Is(err, authentication.ErrInsufficientPermissions):
		logger.WithContext(ctx).WithError(err).Info("User verification failed - insufficient permissions")
		cb(w, r, http.StatusUnauthorized, http.StatusText(http.StatusUnauthorized))
	case errors.Is(err, actions.ErrActionFailed):
		log.WithError(err).Debug("Precondition failed, aborted by action failure")
		cb(w, r, http.StatusPreconditionFailed, err)
	default:
		logger.WithContext(ctx).WithError(err).Error("API call returned status internal server error")
		cb(w, r, http.StatusInternalServerError, err)
	}

	return true
}
