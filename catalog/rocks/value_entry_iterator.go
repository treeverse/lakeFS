package rocks

import (
	"github.com/treeverse/lakefs/graveler"
)

type valueEntryIterator struct {
	it    graveler.ValueIterator
	value *EntryRecord
	err   error
}

func NewValueToEntryIterator(it graveler.ValueIterator) *valueEntryIterator {
	return &valueEntryIterator{
		it: it,
	}
}

func (e *valueEntryIterator) Next() bool {
	if e.err != nil {
		return false
	}
	hasNext := e.it.Next()
	if !hasNext {
		e.value = nil
		e.err = e.it.Err()
		return false
	}
	v := e.it.Value()
	// get entry from value
	entry, err := ValueToEntry(v.Value)
	if err != nil {
		e.value = nil
		e.err = err
		return false
	}
	e.value = &EntryRecord{
		Path:  Path(v.Key),
		Entry: entry,
	}
	return true
}

func (e *valueEntryIterator) SeekGE(id Path) {
	e.value = nil
	var key graveler.Key
	key, e.err = graveler.NewKey(id.String())
	if e.err != nil {
		return
	}
	e.it.SeekGE(key)
}

func (e *valueEntryIterator) Value() *EntryRecord {
	return e.value
}

func (e *valueEntryIterator) Err() error {
	return e.err
}

func (e *valueEntryIterator) Close() {
	e.it.Close()
}
