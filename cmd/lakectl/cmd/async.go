package cmd

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
	"github.com/treeverse/lakefs/pkg/api/helpers"
	"github.com/treeverse/lakefs/pkg/logging"
)

const (
	initialInterval = 1 * time.Second  // initial interval for exponential backoff
	maxInterval     = 10 * time.Second // max interval for exponential backoff

	defaultPollInterval = 3 * time.Second // default interval while pulling tasks status
	minimumPollInterval = time.Second     // minimum interval while pulling tasks status
	defaultPollTimeout  = time.Hour       // default expiry for pull status with no update
)

var ErrTaskNotCompleted = errors.New("task not completed")

type StatusResponse struct {
	Completed bool
}

type asyncStatusCallback func() (*apigen.AsyncTaskStatus, error)

// getErrFromStatus returns a UserVisibleAPIError from AsyncTaskStatus that simulates synchronous operation error response
func getErrFromStatus(resp apigen.AsyncTaskStatus) helpers.UserVisibleAPIError {
	statusCode := int(apiutil.Value(resp.StatusCode))
	status := fmt.Sprintf("%d %s", statusCode, http.StatusText(statusCode))
	return helpers.UserVisibleAPIError{
		APIFields: helpers.APIFields{
			StatusCode: statusCode,
			Status:     status,
			Message:    resp.Error.Message,
		},
	}
}

// TODO (niro): We will need to implement timeout and cancel logic here
func pollAsyncOperationStatus(ctx context.Context, taskID string, operation string, cb asyncStatusCallback) error {
	var bo backoff.BackOff = backoff.NewExponentialBackOff(
		backoff.WithInitialInterval(initialInterval), backoff.WithMaxInterval(maxInterval)) // No timeout
	bo = backoff.WithContext(bo, ctx)
	logging.FromContext(ctx).WithFields(logging.Fields{"task_id": taskID}).Debug(fmt.Sprintf("Checking status of %s", operation))
	return backoff.Retry(func() error {
		resp, err := cb()
		if err != nil {
			return backoff.Permanent(err)
		}
		if resp == nil {
			return backoff.Permanent(fmt.Errorf("nil status %w", helpers.ErrRequestFailed))
		}
		if resp.Error != nil {
			return backoff.Permanent(getErrFromStatus(*resp))
		}
		if resp.Completed {
			return nil
		}
		return ErrTaskNotCompleted
	}, bo)
}
