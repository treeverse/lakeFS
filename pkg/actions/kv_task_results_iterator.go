package actions

import (
	"context"

	"github.com/treeverse/lakefs/pkg/kv"
)

type taskResultIterator struct {
	it    kv.PrimaryIterator
	entry *TaskResult
	err   error
}

// NewKVTaskResultIterator returns a new iterator over actions task results of a specific run
// 'after' determines the hook run ID which we should start the scan from, used for pagination
func NewKVTaskResultIterator(ctx context.Context, store kv.StoreMessage, repositoryID, runID, after string) (*taskResultIterator, error) {
	prefix := TasksPath(repositoryID, runID)
	if after != "" {
		after = kv.FormatPath(prefix, after)
	}
	it, err := kv.NewPrimaryIterator(ctx, store.Store, (&TaskResultData{}).ProtoReflect().Type(), PartitionKey, []byte(prefix), kv.IteratorOptionsAfter([]byte(after)))
	if err != nil {
		return nil, err
	}
	return &taskResultIterator{
		it: *it,
	}, nil
}

func (i *taskResultIterator) Next() bool {
	if i.Err() != nil {
		return false
	}
	if !i.it.Next() {
		i.entry = nil
		return false
	}
	entry := i.it.Entry()
	if entry == nil {
		i.err = ErrNilValue
		return false
	}

	i.entry = taskResultFromProto(entry.Value.(*TaskResultData))
	return true
}

func (i *taskResultIterator) Value() *TaskResult {
	if i.Err() != nil {
		return nil
	}

	return i.entry
}

func (i *taskResultIterator) Err() error {
	if i.err == nil {
		return i.it.Err()
	}
	return i.err
}

func (i *taskResultIterator) Close() {
	i.it.Close()
}
