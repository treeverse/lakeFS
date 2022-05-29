package actions

import (
	"context"

	"github.com/treeverse/lakefs/pkg/kv"
	"google.golang.org/protobuf/proto"
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
		prefix = getByBranchPath(repositoryID, branchID)
	case commitID != "":
		prefix = getByCommitPath(repositoryID, commitID)
	default:
		prefix = kv.FormatPath(getBaseActionsPath(repositoryID), runsPrefix)
		secondary = false
	}

	var it kv.MessageIterator
	if secondary {
		it, err = kv.NewSecondaryIterator(ctx, store.Store, prefix, after)
		if err != nil {
			return nil, err
		}
	} else {
		it, err = kv.NewPrimaryIterator(ctx, store.Store, prefix, after)
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
	var v proto.Message = &RunResultData{}
	err := i.it.Entry(nil, &v)
	if err != nil {
		i.err = err
		return nil
	}
	return runResultFromProto(v.(*RunResultData))
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
