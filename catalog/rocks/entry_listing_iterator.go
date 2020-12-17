package rocks

import (
	"github.com/treeverse/lakefs/graveler"
)

type entryListingIterator struct {
	it    graveler.ListingIterator
	value *EntryListing
	err   error
}

func NewEntryListingIterator(it graveler.ListingIterator) EntryListingIterator {
	return &entryListingIterator{
		it: it,
	}
}

func (e *entryListingIterator) Next() bool {
	if e.err != nil {
		return false
	}
	hasNext := e.it.Next()
	if !hasNext {
		e.err = e.it.Err()
		e.value = nil
		return false
	}
	v := e.it.Value()
	// get entry from value (optional)
	var ent *Entry
	if v.Value != nil {
		ent, e.err = ValueToEntry(v.Value)
		if e.err != nil {
			e.value = nil
			return false
		}
	}
	e.value = &EntryListing{
		CommonPrefix: v.CommonPrefix,
		Path:         Path(v.Key),
		Entry:        ent,
	}
	return true
}

func (e *entryListingIterator) SeekGE(id Path) {
	e.value = nil
	var key graveler.Key
	key, e.err = graveler.NewKey(id.String())
	if e.err != nil {
		return
	}
	e.it.SeekGE(key)
}

func (e *entryListingIterator) Value() *EntryListing {
	return e.value
}

func (e *entryListingIterator) Err() error {
	return e.err
}

func (e *entryListingIterator) Close() {
	e.it.Close()
}
