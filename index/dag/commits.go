package dag

import (
	"github.com/treeverse/lakefs/index/model"
)

type CommitReader interface {
	ReadCommit(addr string) (*model.Commit, error)
}

func BfsScan(reader CommitReader, startAddr string, results int, after string) ([]*model.Commit, bool, error) {
	iter := NewBfsIterator(reader, startAddr)
	commits := make([]*model.Commit, 0)
	passedAfter := after == ""
	for iter.advance() {
		commit, err := iter.get()
		if err != nil {
			return nil, false, err
		}
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
	return commits, iter.hasMore(), nil
}

func FindLowestCommonAncestor(reader CommitReader, addrA, addrB string) (*model.Commit, error) {
	// implementation used to be as per https://stackoverflow.com/a/27285628

	var sentinel = struct{}{}
	discoveredSet := make(map[string]struct{})
	iterA := NewBfsIterator(reader, addrA)
	iterB := NewBfsIterator(reader, addrB)
	hasNextA := iterA.advance()
	hasNextB := iterB.advance()
	for {
		if !hasNextA && !hasNextB {
			// no common ancestor
			return nil, nil
		}
		if hasNextA {
			commit, err := iterA.get()
			if err != nil {
				return nil, err
			}
			if _, wasDiscovered := discoveredSet[commit.Address]; !wasDiscovered {
				discoveredSet[commit.Address] = sentinel
			} else {
				return commit, nil
			}
			hasNextA = iterA.advance()
		}
		if hasNextB {
			commit, err := iterB.get()
			if err != nil {
				return nil, err
			}
			if _, wasDiscovered := discoveredSet[commit.Address]; !wasDiscovered {
				discoveredSet[commit.Address] = sentinel
			} else {
				return commit, nil
			}
			hasNextB = iterB.advance()
		}
	}
}
