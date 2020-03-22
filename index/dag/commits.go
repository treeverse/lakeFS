package dag

import (
	"github.com/treeverse/lakefs/index/model"
)

type CommitReader interface {
	ReadCommit(addr string) (*model.Commit, error)
}

func bfsScan(reader CommitReader, startAddr string, matchOnly []*model.Commit) ([]*model.Commit, error) {
	// make a set of skip nodes for O(1) lookup
	var sentinel = struct{}{}
	matchOnlySet := make(map[string]struct{})
	if matchOnly != nil {
		for _, node := range matchOnly {
			matchOnlySet[node.GetAddress()] = sentinel
		}
	}
	commits := make([]*model.Commit, 0)
	iter := NewBfsIterator(reader, startAddr)
	for iter.advance() {
		commit, err := iter.get()
		if err != nil {
			return nil, err
		}
		if _, isMatch := matchOnlySet[commit.Address]; matchOnly == nil || isMatch {
			commits = append(commits, commit)
		}
	}
	return commits, nil
}

func BfsScan(reader CommitReader, startAddr string) ([]*model.Commit, error) {
	return bfsScan(reader, startAddr, nil)
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
