package actions

import (
	"context"

	"google.golang.org/protobuf/proto"

	"github.com/treeverse/lakefs/pkg/logging"

	"github.com/treeverse/lakefs/pkg/kv"
)

type KVRunResultIterator struct {
	it       kv.MessageIterator
	ctx      context.Context
	branchID string
	commitID string
}

func NewKVRunResultIterator(ctx context.Context, store kv.StoreMessage, repositoryID, branchID, commitID, prefix string) *KVRunResultIterator {
	key := kv.FormatPath(getRunPath(repositoryID), prefix)
	it, err := store.Scan(ctx, key)
	if err != nil {
		return nil
	}
	return &KVRunResultIterator{
		it:       *it,
		ctx:      ctx,
		branchID: branchID,
		commitID: commitID,
	}
}

func (i *KVRunResultIterator) Next() bool {
	entry := kv.NewMessageEntry((&RunResultData{}).ProtoReflect().Type())
	for i.it.Next() {
		err := i.it.Entry(&entry.Key, &entry.Value)
		if err != nil {
			logging.Default().WithError(err).Error("next entry failed")
			return false
		}
		if entry.Value != nil {
			value := entry.Value.(*RunResultData)
			switch {
			case i.branchID != "":
				if value.BranchId == i.branchID {
					return true
				}
			case i.commitID != "":
				if value.CommitId == i.commitID {
					return true
				}
			default: // both branchID and commitID are empty return the first next entry
				return true
			}
		}
	}
	return false
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
