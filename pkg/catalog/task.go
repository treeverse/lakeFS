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
	taskIDNanoLength   = 20
	tasksPrefix        = "tasks"
	instancesPartition = "instances"
)

type TaskStep struct {
	Name string
	Func func(ctx context.Context) error
}

func TaskPath(key string) string {
	return kv.FormatPath(tasksPrefix, key)
}

func instanceHeartbeatPath(instanceID string) []byte {
	return []byte(kv.FormatPath("instances", instanceID))
}

func NewTaskID(prefix string) string {
	return prefix + nanoid.Must(taskIDNanoLength)
}

func IsTaskID(prefix, taskID string) bool {
	return len(taskID) == len(prefix)+taskIDNanoLength && strings.HasPrefix(taskID, prefix)
}

func UpdateTaskStatus(ctx context.Context, kvStore kv.Store, repository *graveler.RepositoryRecord, taskID string, statusMsg protoreflect.ProtoMessage) error {
	return kv.SetMsg(ctx, kvStore, graveler.RepoPartition(repository), []byte(TaskPath(taskID)), statusMsg)
}

func GetTaskStatus(ctx context.Context, kvStore kv.Store, repository *graveler.RepositoryRecord, taskID string, statusMsg protoreflect.ProtoMessage) (kv.Predicate, error) {
	predicate, err := kv.GetMsg(ctx, kvStore, graveler.RepoPartition(repository), []byte(TaskPath(taskID)), statusMsg)
	if err != nil {
		if errors.Is(err, kv.ErrNotFound) {
			return nil, graveler.ErrNotFound
		}
		return nil, err
	}
	return predicate, nil
}
