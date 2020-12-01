package tree

import (
	"github.com/treeverse/lakefs/catalog/rocks"
)

type pushBackEntryIterator struct {
	rocks.EntryIterator
	ignoreNextOnce bool
}

func newPushbackEntryIterator(iterator rocks.EntryIterator) *pushBackEntryIterator {
	return &pushBackEntryIterator{EntryIterator: iterator}
}

func (i *pushBackEntryIterator) Next() bool {
	if i.ignoreNextOnce {
		i.ignoreNextOnce = false
		return true
	}
	return i.EntryIterator.Next()
}

func (i *pushBackEntryIterator) pushBack() error {
	if i.ignoreNextOnce {
		return ErrPushBackTwice
	} else {
		i.ignoreNextOnce = true
		return nil
	}
}
