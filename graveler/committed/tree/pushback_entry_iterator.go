package tree

import (
	gr "github.com/treeverse/lakefs/graveler"
)

type pushBackValueIterator struct {
	gr.ValueIterator
	ignoreNextOnce bool
}

func newPushbackEntryIterator(iterator gr.ValueIterator) *pushBackValueIterator {
	return &pushBackValueIterator{ValueIterator: iterator}
}

func (i *pushBackValueIterator) Next() bool {
	if i.ignoreNextOnce {
		i.ignoreNextOnce = false
		return true
	}
	return i.ValueIterator.Next()
}

func (i *pushBackValueIterator) pushBack() error {
	if i.ignoreNextOnce {
		return ErrPushBackTwice
	} else {
		i.ignoreNextOnce = true
		return nil
	}
}
