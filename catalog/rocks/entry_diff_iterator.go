package rocks

import "github.com/treeverse/lakefs/graveler"

type entryDiffIterator struct {
	it    graveler.DiffIterator
	value *EntryDiff
	err   error
}

func NewEntryDiffIterator(it graveler.DiffIterator) EntryDiffIterator {
	return &entryDiffIterator{
		it: it,
	}
}

func (e entryDiffIterator) Next() bool {
	if e.err != nil {
		return false
	}
	if !e.it.Next() {
		e.value = nil
		e.err = e.it.Err()
		return false
	}
	v := e.it.Value()

	// convert diff value if found to entry
	var entry *Entry
	if v.Value != nil {
		entry, e.err = ValueToEntry(*v.Value)
		if e.err != nil {
			e.value = nil
			return false
		}
	}
	// return entry diff
	e.value = &EntryDiff{
		Type:  v.Type,
		Path:  Path(v.Key),
		Entry: entry,
	}
	return true
}

func (e entryDiffIterator) SeekGE(id Path) {
	e.value = nil
	key, err := graveler.NewKey(id.String())
	if err != nil {
		e.err = err
		return
	}
	e.it.SeekGE(key)
}

func (e entryDiffIterator) Value() *EntryDiff {
	return e.value
}

func (e entryDiffIterator) Err() error {
	return e.err
}

func (e entryDiffIterator) Close() {
	e.it.Close()
}
