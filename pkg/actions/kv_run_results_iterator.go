package actions

import (
	"context"

	"github.com/treeverse/lakefs/pkg/kv"
)

type KVRunResultIterator struct {
	it  kv.MessageIterator
	err error
}

func NewKVRunResultIterator(ctx context.Context, store kv.StoreMessage, repositoryID, branchID, commitID, after string) (*KVRunResultIterator, error) {
	var (
		prefix string
		err    error
	)
	secondary := true

	switch {
	case branchID != "":
		prefix = GetByBranchPath(repositoryID, branchID)
	case commitID != "":
		prefix = GetByCommitPath(repositoryID, commitID)
	default:
		prefix = kv.FormatPath(GetBaseActionsPath(repositoryID), RunsPrefix)
		secondary = false
	}
	if after != "" {
		after = kv.FormatPath(prefix, after)
	}

	var it kv.MessageIterator
	if secondary {
		it, err = kv.NewSecondaryIterator(ctx, store.Store, (&RunResultData{}).ProtoReflect().Type(), prefix, after)
		if err != nil {
			return nil, err
		}
	} else {
		it, err = kv.NewPrimaryIterator(ctx, store.Store, (&RunResultData{}).ProtoReflect().Type(), prefix, after)
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
