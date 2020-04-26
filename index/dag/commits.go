package dag

import (
	"github.com/treeverse/lakefs/index/model"
)

type CommitReader interface {
	ReadCommit(addr string) (*model.Commit, error)
}

func CommitScan(reader CommitReader, startAddr string, results int, after string) ([]*model.Commit, bool, error) {
	iter := NewCommitIterator(reader, startAddr)
	commits := make([]*model.Commit, 0)
	passedAfter := after == ""
	for iter.Next() {
		commit := iter.Value()
		if passedAfter {
			commits = append(commits, commit)
			//result <= 0 is considered as all
			if len(commits) == results {
				break
			}
		} else {
			passedAfter = commit.Address == after
		}
	}
	if iter.Err() != nil {
		return nil, false, iter.Err()
	}
	return commits, iter.Next(), nil
}

// FindLowestCommonAncestor implementation used to be as per https://stackoverflow.com/a/27285628
func FindLowestCommonAncestor(reader CommitReader, addrA, addrB string) (*model.Commit, error) {
	discoveredSet := make(map[string]struct{})
	iterA := NewCommitIterator(reader, addrA)
	iterB := NewCommitIterator(reader, addrB)
	for {
		commit, err := findLowerCommonAncestorNextIter(discoveredSet, iterA)
		if commit != nil || err != nil {
			return commit, err
		}
		commit, err = findLowerCommonAncestorNextIter(discoveredSet, iterB)
		if commit != nil || err != nil {
			return commit, err
		}
		if iterA.Value() == nil && iterB.Value() == nil {
			break
		}
	}
	return nil, nil
}

func findLowerCommonAncestorNextIter(discoveredSet map[string]struct{}, iter *CommitIterator) (*model.Commit, error) {
	if iter.Next() {
		commit := iter.Value()
		if _, wasDiscovered := discoveredSet[commit.Address]; wasDiscovered {
			return commit, nil
		}
		discoveredSet[commit.Address] = struct{}{}
	}
	return nil, iter.Err()
}
