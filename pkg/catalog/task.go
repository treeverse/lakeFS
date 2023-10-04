package catalog

import (
	"context"
	"errors"
	"strings"
	"time"

	nanoid "github.com/matoous/go-nanoid/v2"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/kv"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const (
	TaskExpiryTime          = 24 * time.Hour
	DumpRefsTaskIDPrefix    = "DR"
	RestoreRefsTaskIDPrefix = "RR"
	taskIDNanoLength        = 20
)

func newTaskID(prefix string) string {
	return prefix + nanoid.Must(taskIDNanoLength)
}

func isTaskID(prefix, taskID string) bool {
	return len(taskID) == len(prefix)+taskIDNanoLength && strings.HasPrefix(taskID, prefix)
}

func updateTaskStatus(ctx context.Context, kvStore kv.Store, repository *graveler.RepositoryRecord, taskID string, statusMsg protoreflect.ProtoMessage) error {
	err := kv.SetMsg(ctx, kvStore, graveler.RepoPartition(repository), []byte(TaskPath(taskID)), statusMsg)
	if err != nil {
		return err
	}
	return nil
}

func getTaskStatus(ctx context.Context, kvStore kv.Store, repository *graveler.RepositoryRecord, taskID string, statusMsg protoreflect.ProtoMessage) error {
	_, err := kv.GetMsg(ctx, kvStore, graveler.RepoPartition(repository), []byte(TaskPath(taskID)), statusMsg)
	if err != nil {
		if errors.Is(err, kv.ErrNotFound) {
			return graveler.ErrNotFound
		}
		return err
	}
	return nil
}
