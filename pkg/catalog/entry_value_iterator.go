package catalog

import "github.com/treeverse/lakefs/pkg/graveler"

type entryValueIterator struct {
	it    EntryIterator
	value *graveler.ValueRecord
	err   error
}

func NewEntryToValueIterator(it EntryIterator) *entryValueIterator {
	return &entryValueIterator{
		it: it,
	}
}

func (e *entryValueIterator) Next() bool {
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

	var val *graveler.Value
	if v.Entry != nil {
		val, e.err = EntryToValue(v.Entry)
		if e.err != nil {
			e.value = nil
			return false
		}
	}

	e.value = &graveler.ValueRecord{
		Key:   graveler.Key(v.Path),
		Value: val,
	}
	return true
}

func (e *entryValueIterator) SeekGE(id graveler.Key) {
	e.value = nil
	e.it.SeekGE(Path(id))
}

func (e *entryValueIterator) Value() *graveler.ValueRecord {
	return e.value
}

func (e *entryValueIterator) Err() error {
	return e.err
}

func (e *entryValueIterator) Close() {
	e.it.Close()
}
