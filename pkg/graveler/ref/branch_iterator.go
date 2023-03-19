package ref

import (
	"context"
	"errors"
	"sort"

	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/kv"
)

// CompareFunc type used for sorting in InMemIterator, it is a strictly bigger comparison function required for the
// sort.Slice algorithm, implementors need to decide how to handle equal values
type CompareFunc func(i, j int) bool

var ErrIteratorClosed = errors.New("iterator already closed")

// BranchSimpleIterator Iterates over repository's branches in a sorted way, since the branches are already sorted in DB according to BranchID
type BranchSimpleIterator struct {
	ctx           context.Context
	store         kv.Store
	itr           *kv.PrimaryIterator
	repoPartition string
	value         *graveler.BranchRecord
	err           error
}

func NewBranchSimpleIterator(ctx context.Context, store kv.Store, repo *graveler.RepositoryRecord) (*BranchSimpleIterator, error) {
	repoPartition := graveler.RepoPartition(repo)
	it, err := kv.NewPrimaryIterator(ctx, store, (&graveler.BranchData{}).ProtoReflect().Type(),
		repoPartition, []byte(graveler.BranchPath("")), kv.IteratorOptionsFrom([]byte("")))
	if err != nil {
		return nil, err
	}

	return &BranchSimpleIterator{
		ctx:           ctx,
		store:         store,
		itr:           it,
		repoPartition: repoPartition,
		value:         nil,
		err:           nil,
	}, nil
}

func (bi *BranchSimpleIterator) Next() bool {
	if bi.Err() != nil {
		return false
	}
	if !bi.itr.Next() {
		bi.value = nil
		return false
	}
	entry := bi.itr.Entry()
	if entry == nil {
		bi.err = graveler.ErrInvalid
		return false
	}
	value, ok := entry.Value.(*graveler.BranchData)
	if !ok {
		bi.err = graveler.ErrReadingFromStore
		return false
	}

	bi.value = &graveler.BranchRecord{
		BranchID: graveler.BranchID(value.Id),
		Branch:   branchFromProto(value),
	}
	return true
}

func (bi *BranchSimpleIterator) SeekGE(id graveler.BranchID) {
	if bi.Err() == nil {
		bi.itr.Close() // Close previous before creating new iterator
		bi.itr, bi.err = kv.NewPrimaryIterator(bi.ctx, bi.store, (&graveler.BranchData{}).ProtoReflect().Type(),
			bi.repoPartition, []byte(graveler.BranchPath("")), kv.IteratorOptionsFrom([]byte(graveler.BranchPath(id))))
	}
}

func (bi *BranchSimpleIterator) Value() *graveler.BranchRecord {
	if bi.Err() != nil {
		return nil
	}
	return bi.value
}

func (bi *BranchSimpleIterator) Err() error {
	if bi.err != nil {
		return bi.err
	}
	return bi.itr.Err()
}

func (bi *BranchSimpleIterator) Close() {
	bi.err = ErrIteratorClosed
	if bi.itr != nil {
		bi.itr.Close()
	}
}

// BranchByCommitIterator iterates over repository's branches ordered by Commit ID. Currently, implemented as in-mem iterator
type BranchByCommitIterator struct {
	ctx    context.Context
	values []*graveler.BranchRecord
	value  *graveler.BranchRecord
	idx    int
	err    error
}

func (b *BranchByCommitIterator) SortByCommitID(i, j int) bool {
	return b.values[i].CommitID.String() <= b.values[j].CommitID.String()
}

func NewBranchByCommitIterator(ctx context.Context, store kv.Store, repo *graveler.RepositoryRecord) (*BranchByCommitIterator, error) {
	bi := &BranchByCommitIterator{
		ctx:    ctx,
		values: make([]*graveler.BranchRecord, 0),
	}
	itr, err := NewBranchSimpleIterator(ctx, store, repo)
	if err != nil {
		return nil, err
	}
	defer itr.Close()
	for itr.Next() {
		value := itr.Value()
		if value == nil { // nil only if internal itr has errors
			return nil, itr.Err()
		}
		bi.values = append(bi.values, itr.Value())
	}
	if itr.Err() != nil {
		return nil, err
	}

	sort.Slice(bi.values, bi.SortByCommitID)
	return bi, nil
}

func (b *BranchByCommitIterator) Next() bool {
	if b.idx >= len(b.values) {
		return false
	}
	b.value = b.values[b.idx]
	b.idx++
	return true
}

func (b *BranchByCommitIterator) SeekGE(_ graveler.BranchID) {
	panic("Not Implemented")
}

func (b *BranchByCommitIterator) Value() *graveler.BranchRecord {
	if b.Err() != nil {
		return nil
	}
	return b.value
}

func (b *BranchByCommitIterator) Err() error {
	return b.err
}

func (b *BranchByCommitIterator) Close() {
	b.err = ErrIteratorClosed
}
