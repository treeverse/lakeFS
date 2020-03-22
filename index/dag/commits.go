package dag

import (
	"sort"

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

func FindLowestCommonAncestor(reader CommitReader, addrA, addrB string) ([]*model.Commit, error) {
	// implementation as per https://stackoverflow.com/a/27285628
	// TODO: there's a more efficient way to do this that doesn't require running full BFS for each one of nodes.
	blueNodes, err := bfsScan(reader, addrA, nil)
	if err != nil {
		return nil, err
	}
	redNodes, err := bfsScan(reader, addrB, blueNodes)
	if err != nil {
		return nil, err
	}

	nodeMap := make(map[string]*model.Commit)
	for _, redNode := range redNodes {
		nodeMap[redNode.GetAddress()] = redNode
	}

	var counts = make(map[string]int)
	var commitMap = make(map[string]*model.Commit)
	for _, commit := range redNodes {
		commitMap[commit.GetAddress()] = commit
		if _, exists := counts[commit.GetAddress()]; !exists {
			counts[commit.GetAddress()] = 0
		}
		for _, parent := range commit.GetParents() {
			if _, exists := counts[parent]; !exists {
				counts[parent] = 1
			} else {
				counts[parent] = counts[parent] + 1
			}
		}
	}
	// filter all nodes whose count is 0
	candidates := make([]*model.Commit, 0)
	for addr, count := range counts {
		if count == 0 {
			candidates = append(candidates, commitMap[addr])
		}
	}

	sort.SliceStable(candidates, func(i, j int) bool {
		return candidates[i].GetAddress() < candidates[j].GetAddress()
	})

	return candidates, nil
}
