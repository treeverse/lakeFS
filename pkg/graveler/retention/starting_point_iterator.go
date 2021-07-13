package retention

import "github.com/treeverse/lakefs/pkg/graveler"

// A GCStartingPoint represents a commit from which the GC algorithm should start scanning.
// It could be either a branch HEAD, or a dangling commit.
// The CommitID field is always set, while BranchID is set only if the commit is a branch HEAD.
type GCStartingPoint struct {
	BranchID graveler.BranchID
	CommitID graveler.CommitID
}

// GCStartingPointIterator combines a branch iterator and a commit iterator. Both are assumed to be sorted and to contain no duplicates.
// Each returned GCStartingPoint object contains the commit id. If the entry came from the branch iterator, it also contains the branch id.
// Commits appearing in both iterators appear only once and include the branch id.
// Closing this iterator will close the two internal iterators.
type GCStartingPointIterator struct {
	commitIterator graveler.CommitIterator
	branchIterator graveler.BranchIterator

	commitValue *graveler.CommitRecord
	branchValue *graveler.BranchRecord
	value       *GCStartingPoint
}

func NewGCStartingPointIterator(commitIterator graveler.CommitIterator, branchIterator graveler.BranchIterator) *GCStartingPointIterator {
	return &GCStartingPointIterator{commitIterator: commitIterator, branchIterator: branchIterator}
}

func (sp *GCStartingPointIterator) Next() bool {
	if sp.branchValue == nil && sp.branchIterator.Next() {
		sp.branchValue = sp.branchIterator.Value()
	}
	if sp.commitValue == nil && sp.commitIterator.Next() {
		sp.commitValue = sp.commitIterator.Value()
	}
	if sp.branchValue == nil && sp.commitValue == nil {
		return false
	}
	if sp.branchValue != nil && sp.commitValue != nil && sp.branchValue.CommitID == sp.commitValue.CommitID {
		// commit is same as branch head - skip the commit
		sp.commitValue = nil
	}
	if sp.commitValue == nil || (sp.branchValue != nil && sp.commitValue.CommitID > sp.branchValue.CommitID) {
		// has only branch, or branch is before commit
		sp.value = &GCStartingPoint{
			BranchID: sp.branchValue.BranchID,
			CommitID: sp.branchValue.CommitID,
		}
		sp.branchValue = nil
		return true
	}
	if sp.branchValue == nil || sp.commitValue.CommitID < sp.branchValue.CommitID {
		// has only commit, or commit is before branch
		sp.value = &GCStartingPoint{
			CommitID: sp.commitValue.CommitID,
		}
		sp.commitValue = nil
		return true
	}
	return false
}

func (sp *GCStartingPointIterator) Value() *GCStartingPoint {
	return sp.value
}

func (sp *GCStartingPointIterator) Err() error {
	if sp.branchIterator != nil {
		return sp.branchIterator.Err()
	}
	return sp.commitIterator.Err()
}

func (sp *GCStartingPointIterator) Close() {
	sp.commitIterator.Close()
	sp.branchIterator.Close()
}
