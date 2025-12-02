package cmd

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/helpers"
	"github.com/treeverse/lakefs/pkg/logging"
)

const (
	initialInterval = 1 * time.Second  // initial interval for exponential backoff
	MaxInterval     = 10 * time.Second // max interval for exponential backoff

	defaultPollInterval = 3 * time.Second // default interval while pulling tasks status
	minimumPollInterval = time.Second     // minimum interval while pulling tasks status
	defaultPollTimeout  = time.Hour       // default expiry for pull status with no update
)

var (
	ErrTaskNotCompleted = errors.New("task not completed")
	ErrTaskFailed       = errors.New("task failed")
)

type StatusResponse struct {
	Completed bool
}

type asyncStatusCallback func() (*apigen.AsyncTaskStatus, error)

// TODO (niro): We will need to implement timeout and cancel logic here
func pollAsyncOperationStatus(ctx context.Context, taskID string, operation string, cb asyncStatusCallback) error {
	var bo backoff.BackOff = backoff.NewExponentialBackOff(
		backoff.WithInitialInterval(initialInterval), backoff.WithMaxInterval(MaxInterval)) // No timeout
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
		if resp.Completed {
			return nil
		}
		if resp.Error != nil {
			return backoff.Permanent(fmt.Errorf("%s: %w", resp.Error.Message, ErrTaskFailed))
		}
		return ErrTaskNotCompleted
	}, bo)
}
