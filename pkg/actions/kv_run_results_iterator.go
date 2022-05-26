package actions

import (
	"context"

	"google.golang.org/protobuf/proto"

	"github.com/treeverse/lakefs/pkg/logging"

	"github.com/treeverse/lakefs/pkg/kv"
)

type KVRunResultIterator struct {
	it  kv.MessageIterator
	ctx context.Context
}

func NewKVRunResultIterator(ctx context.Context, store kv.StoreMessage, repositoryID, branchID, commitID, prefix string) *KVRunResultIterator {
	var key string
	var err error
	secondary := true

	switch {
	case branchID != "":
		key = getRunByBranchPath(repositoryID, branchID)
	case commitID != "":
		key = getRunByCommitPath(repositoryID, commitID)
	default:
		key = getRunPath(repositoryID, prefix)
		secondary = false
	}

	var it kv.MessageIterator
	if secondary {
		it, err = kv.NewSecondaryIterator(ctx, store.Store, key)
		if err != nil {
			return nil
		}
	} else {
		it, err = kv.NewPrimaryIterator(ctx, store.Store, key)
		if err != nil {
			return nil
		}
	}

	return &KVRunResultIterator{
		it:  it,
		ctx: ctx,
	}
}

func (i *KVRunResultIterator) Next() bool {
	return i.it.Next()
}

func (i *KVRunResultIterator) Value() *RunResult {
	if i.Err() != nil {
		return nil
	}
	var v proto.Message = &RunResultData{}
	err := i.it.Entry(nil, &v)
	if err != nil {
		logging.Default().WithError(err).Errorf("value failed")
	}
	return runResultFromProto(v.(*RunResultData))
}

func (i *KVRunResultIterator) Err() error {
	return i.it.Err()
}
func (i *KVRunResultIterator) Close() {
	i.it.Close()
}
