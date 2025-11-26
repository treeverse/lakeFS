package catalog

import (
	"context"
	"errors"
	"strings"

	nanoid "github.com/matoous/go-nanoid/v2"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/kv"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const (
	taskIDNanoLength = 20
	tasksPrefix      = "tasks"
)

// Error codes for task errors - used to map errors to HTTP status codes
const (
	ErrorCodeBadRequest          = "BAD_REQUEST"
	ErrorCodeNotFound            = "NOT_FOUND"
	ErrorCodeConflict            = "CONFLICT"
	ErrorCodeForbidden           = "FORBIDDEN"
	ErrorCodeUnauthorized        = "UNAUTHORIZED"
	ErrorCodeGone                = "GONE"
	ErrorCodeTooManyRequests     = "TOO_MANY_REQUESTS"
	ErrorCodeServiceUnavailable  = "SERVICE_UNAVAILABLE"
	ErrorCodePreconditionFailed  = "PRECONDITION_FAILED"
	ErrorCodeNotImplemented      = "NOT_IMPLEMENTED"
	ErrorCodeInternalServerError = "INTERNAL_SERVER_ERROR"
	ErrorCodeClientClosedRequest = "CLIENT_CLOSED_REQUEST"
)

// TaskStep represents a step in a background task
type TaskStep struct {
	Name string
	Func func(ctx context.Context) error
}

// taskStep is an alias for backward compatibility with existing code
type taskStep = TaskStep

func TaskPath(key string) string {
	return kv.FormatPath(tasksPrefix, key)
}

func NewTaskID(prefix string) string {
	return prefix + nanoid.Must(taskIDNanoLength)
}

func IsTaskID(prefix, taskID string) bool {
	return len(taskID) == len(prefix)+taskIDNanoLength && strings.HasPrefix(taskID, prefix)
}

func UpdateTaskStatus(ctx context.Context, kvStore kv.Store, repository *graveler.RepositoryRecord, taskID string, statusMsg protoreflect.ProtoMessage) error {
	err := kv.SetMsg(ctx, kvStore, graveler.RepoPartition(repository), []byte(TaskPath(taskID)), statusMsg)
	if err != nil {
		return err
	}
	return nil
}

func GetTaskStatus(ctx context.Context, kvStore kv.Store, repository *graveler.RepositoryRecord, taskID string, statusMsg protoreflect.ProtoMessage) error {
	_, err := kv.GetMsg(ctx, kvStore, graveler.RepoPartition(repository), []byte(TaskPath(taskID)), statusMsg)
	if err != nil {
		if errors.Is(err, kv.ErrNotFound) {
			return graveler.ErrNotFound
		}
		return err
	}
	return nil
}
