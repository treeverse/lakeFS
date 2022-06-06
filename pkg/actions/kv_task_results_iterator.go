package actions

import (
	"context"

	"github.com/treeverse/lakefs/pkg/kv"
)

type KVTaskResultIterator struct {
	it  kv.PrimaryIterator
	err error
}

// NewKVTaskResultIterator returns a new iterator over actions task results of a specific run
// 'after' determines the hook run ID which we should start the scan from, used for pagination
func NewKVTaskResultIterator(ctx context.Context, store kv.StoreMessage, repositoryID, runID, after string) (*KVTaskResultIterator, error) {
	prefix := TasksPath(repositoryID, runID)
	if after != "" {
		after = kv.FormatPath(prefix, after)
	}
	it, err := kv.NewPrimaryIterator(ctx, store.Store, (&TaskResultData{}).ProtoReflect().Type(), PartitionKey, prefix, after)
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
	entry := i.it.Entry()
	if entry == nil {
		i.err = ErrNilValue
		return nil
	}
	return taskResultFromProto(entry.Value.(*TaskResultData))
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
