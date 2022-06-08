package actions

import (
	"context"
	"errors"

	"github.com/treeverse/lakefs/pkg/kv"
)

var ErrParamConflict = errors.New("parameters conflict")

type KVRunResultIterator struct {
	it  kv.MessageIterator
	err error
}

// NewKVRunResultIterator returns a new iterator over actions run results
// 'after' determines the runID which we should start the scan from, used for pagination
func NewKVRunResultIterator(ctx context.Context, store kv.StoreMessage, repositoryID, branchID, commitID, after string) (*KVRunResultIterator, error) {
	if branchID != "" && commitID != "" {
		return nil, ErrParamConflict
	}

	var (
		prefix string
		err    error
	)
	secondary := true

	switch {
	case branchID != "":
		prefix = ByBranchPath(repositoryID, branchID)
	case commitID != "":
		prefix = ByCommitPath(repositoryID, commitID)
	default:
		prefix = kv.FormatPath(BaseActionsPath(repositoryID), RunsPrefix)
		secondary = false
	}
	if after != "" {
		after = kv.FormatPath(prefix, after)
	}

	var it kv.MessageIterator
	if secondary {
		it, err = kv.NewSecondaryIterator(ctx, store.Store, (&RunResultData{}).ProtoReflect().Type(), PartitionKey, prefix, after)
		if err != nil {
			return nil, err
		}
	} else {
		it, err = kv.NewPrimaryIterator(ctx, store.Store, (&RunResultData{}).ProtoReflect().Type(), PartitionKey, prefix, after)
		if err != nil {
			return nil, err
		}
	}

	return &KVRunResultIterator{
		it: it,
	}, nil
}

func (i *KVRunResultIterator) Next() bool {
	if i.Err() != nil {
		return false
	}
	return i.it.Next()
}

func (i *KVRunResultIterator) Value() *RunResult {
	if i.Err() != nil {
		return nil
	}
	e := i.it.Entry()
	if e == nil {
		return nil
	}
	return runResultFromProto(e.Value.(*RunResultData))
}

func (i *KVRunResultIterator) Err() error {
	if i.err == nil {
		return i.it.Err()
	}
	return i.err
}

func (i *KVRunResultIterator) Close() {
	i.it.Close()
}
