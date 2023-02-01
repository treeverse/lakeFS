package actions

import (
	"context"
	"errors"
	"fmt"

	"github.com/treeverse/lakefs/pkg/kv"
)

var ErrParamConflict = errors.New("parameters conflict")

type KVRunResultIterator struct {
	it    kv.MessageIterator
	err   error
	entry *RunResult
}

// NewKVRunResultIterator returns a new iterator over actions run results
// 'after' determines the runID which we should start the scan from, used for pagination
func NewKVRunResultIterator(ctx context.Context, store kv.Store, repositoryID, branchID, commitID, after string) (*KVRunResultIterator, error) {
	if branchID != "" && commitID != "" {
		return nil, fmt.Errorf("can't use both branchID and CommitID: %w", ErrParamConflict)
	}

	var (
		prefix string
		err    error
	)
	secondary := true

	switch {
	case branchID != "":
		prefix = byBranchPath(repositoryID, branchID)
	case commitID != "":
		prefix = byCommitPath(repositoryID, commitID)
	default:
		prefix = kv.FormatPath(baseActionsPath(repositoryID), runsPrefix)
		secondary = false
	}
	if after != "" {
		after = kv.FormatPath(prefix, after)
	}

	var it kv.MessageIterator
	if secondary {
		it, err = kv.NewSecondaryIterator(ctx, store, (&RunResultData{}).ProtoReflect().Type(), PartitionKey, []byte(prefix), []byte(after))
	} else {
		it, err = kv.NewPrimaryIterator(ctx, store, (&RunResultData{}).ProtoReflect().Type(), PartitionKey, []byte(prefix), kv.IteratorOptionsAfter([]byte(after)))
	}
	if err != nil {
		return nil, err
	}

	return &KVRunResultIterator{
		it: it,
	}, nil
}

func (i *KVRunResultIterator) Next() bool {
	if i.Err() != nil {
		return false
	}
	if !i.it.Next() {
		i.entry = nil
		return false
	}
	e := i.it.Entry()
	if e == nil {
		i.err = ErrNilValue
		return false
	}
	i.entry = RunResultFromProto(e.Value.(*RunResultData))
	return true
}

func (i *KVRunResultIterator) Value() *RunResult {
	if i.Err() != nil {
		return nil
	}
	return i.entry
}

func (i *KVRunResultIterator) Err() error {
	if i.err != nil {
		return i.err
	}
	return i.it.Err()
}

func (i *KVRunResultIterator) Close() {
	i.it.Close()
}
