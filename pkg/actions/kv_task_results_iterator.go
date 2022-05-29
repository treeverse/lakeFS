package actions

import (
	"context"

	"github.com/treeverse/lakefs/pkg/kv"
	"google.golang.org/protobuf/proto"
)

type KVTaskResultIterator struct {
	it  kv.PrimaryIterator
	err error
}

func NewKVTaskResultIterator(ctx context.Context, store kv.StoreMessage, repositoryID, runID, after string) (*KVTaskResultIterator, error) {
	key := kv.FormatPath(getTasksPath(repositoryID, runID), after)
	it, err := store.Scan(ctx, key)
	if err != nil {
		return nil, err
	}
	return &KVTaskResultIterator{
		it: *it,
	}, nil
}

func (i *KVTaskResultIterator) Next() bool {
	if i.Err() != nil {
		return false
	}
	return i.it.Next()
}

func (i *KVTaskResultIterator) Value() *TaskResult {
	if i.Err() != nil {
		return nil
	}
	var v proto.Message = &TaskResultData{}
	err := i.it.Entry(nil, &v)
	if err != nil {
		i.err = err
		return nil
	}
	return taskResultFromProto(v.(*TaskResultData))
}

func (i *KVTaskResultIterator) Err() error {
	if i.err == nil {
		return i.it.Err()
	}
	return i.err
}

func (i *KVTaskResultIterator) Close() {
	i.it.Close()
}
