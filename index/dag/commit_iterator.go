package dag

import "github.com/treeverse/lakefs/index/model"

type CommitIterator struct {
	reader        CommitReader
	queue         []string
	discoveredSet map[string]struct{}
	value         *model.Commit
	err           error
}

func NewCommitIterator(reader CommitReader, startAddr string) *CommitIterator {
	return &CommitIterator{reader: reader, queue: []string{startAddr}, discoveredSet: make(map[string]struct{})}
}

func (c *CommitIterator) Next() bool {
	if c.err != nil || len(c.queue) == 0 {
		c.value = nil
		return false
	}

	// pop
	addr := c.queue[0]
	c.queue = c.queue[1:]
	commit, err := c.reader.ReadCommit(addr)
	if err != nil {
		c.err = err
		c.value = nil
		return false
	}

	//fill queue
	sentinel := struct{}{}
	for _, parent := range commit.Parents {
		if _, wasDiscovered := c.discoveredSet[parent]; !wasDiscovered {
			c.queue = append(c.queue, parent)
			c.discoveredSet[parent] = sentinel
		}
	}
	c.value = commit
	return true
}

func (c *CommitIterator) Value() *model.Commit {
	return c.value
}

func (c *CommitIterator) Err() error {
	return c.err
}
