package dag

import "github.com/treeverse/lakefs/index/model"

type bfsIterator struct {
	reader        CommitReader
	queue         []string
	discoveredSet map[string]struct{}
	item          *model.Commit
	itemError     error
}

func NewBfsIterator(reader CommitReader, startAddr string) *bfsIterator {
	q := make([]string, 0)
	q = append(q, startAddr)
	return &bfsIterator{reader: reader, queue: q, discoveredSet: make(map[string]struct{})}
}

func (bfsIt *bfsIterator) advance() bool {

	if len(bfsIt.queue) == 0 {
		// TODO: consider returning an error | something like end of...
		return false
	}
	var sentinel = struct{}{}
	// pop
	addr := bfsIt.queue[0]
	bfsIt.queue = bfsIt.queue[1:]

	commit, err := bfsIt.reader.ReadCommit(addr)
	if err != nil {
		bfsIt.itemError = err
		return false
	}
	//fill queue
	for _, parent := range commit.GetParents() {
		if _, wasDiscovered := bfsIt.discoveredSet[parent]; !wasDiscovered {
			bfsIt.queue = append(bfsIt.queue, parent)
			bfsIt.discoveredSet[parent] = sentinel
		}
	}
	bfsIt.item = commit
	return true
}

func (bfsIt *bfsIterator) get() (*model.Commit, error) {
	return bfsIt.item, bfsIt.itemError
}
