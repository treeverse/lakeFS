package retention

import "github.com/treeverse/lakefs/pkg/graveler"

type Commit struct {
	commitID      graveler.CommitID
	retentionDays int32
}

// CommitIterator iterates over a repository's branch HEADs, and then all the branch's leaves.
type CommitIterator struct {
	branchIterator graveler.BranchIterator
	leafIterator   graveler.CommitIterator
	hasBranch      bool
	rules          *graveler.GarbageCollectionRules
}

func (l *CommitIterator) Next() bool {
	if l.branchIterator.Next() {
		l.hasBranch = true
		return true
	}
	l.hasBranch = false
	return l.leafIterator.Next()
}

func (l *CommitIterator) Value() Commit {

	if l.hasBranch {
		branchRecord := l.branchIterator.Value()
		commitID := branchRecord.CommitID
		l.rules.BranchRetentionDays[branchRecord.BranchID]
	}
	return l.leafIterator.Value().CommitID
}

func (l *CommitIterator) Err() error {
	if l.branchIterator.Err() != nil {
		return l.branchIterator.Err()
	}
	return l.leafIterator.Err()
}

func (l *CommitIterator) Close() {
	l.branchIterator.Close()
	l.leafIterator.Close()
}
