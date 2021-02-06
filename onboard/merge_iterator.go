package onboard

import (
	"errors"
	"strings"

	"github.com/treeverse/lakefs/catalog"
)

// prefixMergeIterator takes inventory iterator and commit iterator and merges them.
// If an entry's prefix is in `prefixes`, it would be taken from invIt.
// No entry from committedIt would be returned if its prefix is in prefixes.
// The prefixes behaviour guarantees that the two iterators are distinct.
type prefixMergeIterator struct {
	invIt       catalog.EntryIterator
	committedIt catalog.EntryIterator

	err         error
	bothDone    bool
	initialized bool
}

var ErrNotSeekable = errors.New("iterator isn't seekable")

func newPrefixMergeIterator(invIt catalog.EntryIterator, committedIt catalog.EntryListingIterator, prefixes []string) catalog.EntryIterator {
	if len(prefixes) == 0 {
		// If there isn't a filter, take everything from the inventory
		return invIt
	}

	return &prefixMergeIterator{
		invIt:       newPrefixIterator(invIt, prefixes, true),
		committedIt: newPrefixIterator(&listToEntryIterator{EntryListingIterator: committedIt}, prefixes, false),
		bothDone:    false,
		initialized: false,
	}
}

func (pmi *prefixMergeIterator) Next() bool {
	if pmi.err != nil {
		return false
	}
	if !pmi.initialized {
		pmi.initialized = true
		invHasMore := pmi.advanceIt(pmi.invIt)
		committedHasMore := pmi.advanceIt(pmi.committedIt)
		return invHasMore || committedHasMore
	}

	switch {
	case pmi.invIt.Value() == nil && pmi.committedIt.Value() == nil:
		pmi.bothDone = true
		return false

	case pmi.invIt.Value() == nil:
		return pmi.advanceIt(pmi.committedIt)

	case pmi.committedIt.Value() == nil:
		return pmi.advanceIt(pmi.invIt)

	// from now on - both iterators have values,
	// so we'll always return true
	case strings.Compare(pmi.committedIt.Value().Path.String(), pmi.invIt.Value().Path.String()) < 0:
		_ = pmi.advanceIt(pmi.committedIt)
		return true

	default:
		_ = pmi.advanceIt(pmi.invIt)
		return true
	}
}

func (pmi *prefixMergeIterator) advanceIt(it catalog.EntryIterator) bool {
	hasNext := it.Next()
	pmi.err = it.Err()
	return hasNext
}

func (pmi *prefixMergeIterator) SeekGE(_ catalog.Path) {
	pmi.err = ErrNotSeekable
}

func (pmi *prefixMergeIterator) Value() *catalog.EntryRecord {
	if pmi.err != nil || pmi.bothDone {
		return nil
	}

	committedVal := pmi.committedIt.Value()
	invVal := pmi.invIt.Value()

	if committedVal == nil {
		return invVal
	}
	if invVal == nil {
		return committedVal
	}

	if strings.Compare(committedVal.Path.String(), invVal.Path.String()) < 0 {
		return committedVal
	}

	return invVal
}

func (pmi *prefixMergeIterator) Err() error {
	return pmi.err
}

func (pmi *prefixMergeIterator) Close() {
	pmi.invIt.Close()
	if pmi.err != nil {
		pmi.err = pmi.invIt.Err()
	}

	pmi.committedIt.Close()
	if pmi.err != nil {
		pmi.err = pmi.committedIt.Err()
	}
}

type prefixIterator struct {
	it       catalog.EntryIterator
	prefixes []string

	err           error
	value         *catalog.EntryRecord
	allowPrefixes bool
}

func newPrefixIterator(it catalog.EntryIterator, prefixes []string, allowPrefixes bool) catalog.EntryIterator {
	return &prefixIterator{
		it:            it,
		prefixes:      prefixes,
		allowPrefixes: allowPrefixes,
	}
}

func (pi *prefixIterator) Next() bool {
	if pi.err != nil {
		return false
	}

	for pi.it.Next() {
		// iterate until finding the matching an entry
		// that doesn't start with one of the prefixes.
		val := pi.it.Value()
		if pi.include(val) {
			pi.value = &catalog.EntryRecord{
				Path:  val.Path,
				Entry: val.Entry,
			}
			return true
		}
	}

	// reached the end of the iterator
	pi.err = pi.it.Err()
	pi.value = nil
	return false
}

func (pi *prefixIterator) include(val *catalog.EntryRecord) bool {
	for _, p := range pi.prefixes {
		if strings.HasPrefix(val.Path.String(), p) {
			return pi.allowPrefixes
		}
	}

	return !pi.allowPrefixes
}

func (pi *prefixIterator) SeekGE(_ catalog.Path) {
	pi.err = ErrNotSeekable
}

func (pi *prefixIterator) Value() *catalog.EntryRecord {
	if pi.err != nil {
		return nil
	}

	return pi.value
}

func (pi *prefixIterator) Err() error {
	return pi.err
}

func (pi *prefixIterator) Close() {
	pi.it.Close()
	pi.err = pi.it.Err()
}

type listToEntryIterator struct {
	catalog.EntryListingIterator
}

func (iter *listToEntryIterator) Value() *catalog.EntryRecord {
	val := iter.EntryListingIterator.Value()
	if val == nil {
		return nil
	}

	return &catalog.EntryRecord{
		Path:  val.Path,
		Entry: val.Entry,
	}
}
