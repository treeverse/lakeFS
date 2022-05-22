package actions

import (
	"context"

	"google.golang.org/protobuf/proto"

	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/logging"
)

type KVTaskResultIterator struct {
	it  kv.MessageIterator
	ctx context.Context
}

func NewKVTaskResultIterator(ctx context.Context, store kv.StoreMessage, repositoryID, runID, after string) *KVTaskResultIterator {
	key := kv.FormatPath(getTasksPath(repositoryID, runID), after)
	it, err := store.Scan(ctx, key)
	if err != nil {
		return nil
	}
	return &KVTaskResultIterator{
		it:  *it,
		ctx: ctx,
	}
}

func (i *KVTaskResultIterator) Next() bool {
	return i.it.Next()
}

func (i *KVTaskResultIterator) Value() *TaskResult {
	if i.Err() != nil {
		return nil
	}
	var v proto.Message = &TaskResultData{}
	err := i.it.Entry(nil, &v)
	if err != nil {
		logging.Default().WithError(err).Errorf("value failed")
	}
	return taskResultFromProto(v.(*TaskResultData))
}

func (i *KVTaskResultIterator) Err() error {
	return i.it.Err()
}
func (i *KVTaskResultIterator) Close() {
	i.it.Close()
}
