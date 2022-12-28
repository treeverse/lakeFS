package catalog

import (
	"context"

	"github.com/treeverse/lakefs/pkg/graveler"
)

type UncommittedIterator struct {
	store     Store
	ctx       context.Context
	err       error
	branchItr graveler.BranchIterator
	entryItr  *valueEntryIterator
	branch    *graveler.BranchRecord
	entry     *UncommittedRecord
}

type UncommittedRecord struct {
	branchID graveler.BranchID
	*EntryRecord
}

func NewUncommittedIterator(ctx context.Context, store Store, repository *graveler.RepositoryRecord) (*UncommittedIterator, error) {
	bItr, err := store.ListBranches(ctx, repository)
	if err != nil {
		return nil, err
	}
	return &UncommittedIterator{
		store:     store,
		ctx:       ctx,
		branchItr: bItr,
	}, nil
}

// nextStaging reads the next branch staging area into entryItr
func (u *UncommittedIterator) nextStaging() bool {
	if u.entryItr != nil {
		u.entryItr.Close()
	}
	u.branch = u.branchItr.Value()
	vItr, err := u.store.ListStaging(u.ctx, u.branch.Branch, 0)
	if err != nil {
		u.err = err
		return false
	}
	u.entryItr = NewValueToEntryIterator(vItr)
	return true
}

// next Sets iterators to provide the next entry. Handles dependency between branch and value iterators.
// Sets value and returns true if next entry available - false otherwise
func (u *UncommittedIterator) next() bool {
	u.entry = nil // will stay nil as long as no new value found
	for u.entry == nil {
		if !u.branchItr.Next() || !u.nextStaging() {
			return false
		}
		if u.entryItr.Next() {
			u.entry = &UncommittedRecord{
				branchID: u.branchItr.Value().BranchID,
			}
			u.entry.EntryRecord = u.entryItr.Value()
			return true
		}
		u.err = u.entryItr.Err()
		u.entryItr.Close()
		if u.err != nil {
			return false
		}
	}
	return false // not reachable
}

// Next returns the next entry - if entryItr is still valid - gets the next value from it otherwise call u.next
func (u *UncommittedIterator) Next() bool {
	if u.Err() != nil {
		return false
	}

	if u.entryItr == nil {
		return u.next()
	}
	if u.entryItr.Next() {
		u.entry = &UncommittedRecord{
			branchID: u.branchItr.Value().BranchID,
		}
		u.entry.EntryRecord = u.entryItr.Value()
		return true
	}
	u.err = u.entryItr.Err()
	u.entryItr.Close()
	if u.err != nil {
		return false
	}
	return u.next()
}

func (u *UncommittedIterator) SeekGE(branchID graveler.BranchID, id Path) {
	if u.Err() != nil {
		return
	}
	u.entry = nil
	if u.branch == nil || branchID != u.branch.BranchID {
		u.branchItr.SeekGE(branchID)
		if u.branchItr.Next() && u.nextStaging() {
			u.entryItr.SeekGE(id)
		}
	}
}

func (u *UncommittedIterator) Value() *UncommittedRecord {
	if u.Err() != nil {
		return nil
	}
	return u.entry
}

func (u *UncommittedIterator) Err() error {
	if u.entryItr != nil && u.entryItr.Err() != nil {
		return u.entryItr.Err()
	}
	if u.branchItr.Err() != nil {
		return u.branchItr.Err()
	}
	return u.err
}

func (u *UncommittedIterator) Close() {
	u.branchItr.Close()
	if u.entryItr != nil {
		u.entryItr.Close()
	}
}
