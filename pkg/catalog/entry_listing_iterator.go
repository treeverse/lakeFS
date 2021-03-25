package catalog

import (
	"strings"

	"github.com/treeverse/lakefs/pkg/graveler"
)

type entryListingIterator struct {
	it        EntryIterator
	prefix    string
	delimiter string
	nextFunc  func() bool
	value     *EntryListing
}

func NewEntryListingIterator(it EntryIterator, prefix Path, delimiter Path) EntryListingIterator {
	eli := &entryListingIterator{
		it:        NewPrefixIterator(it, prefix),
		prefix:    prefix.String(),
		delimiter: delimiter.String(),
	}
	if len(delimiter) == 0 {
		eli.nextFunc = eli.nextNoDelimiter
	} else {
		eli.nextFunc = eli.nextWithDelimiter
	}
	return eli
}

func (e *entryListingIterator) Next() bool {
	return e.nextFunc()
}

func (e *entryListingIterator) SeekGE(id Path) {
	e.value = nil
	e.it.SeekGE(id)
}

func (e *entryListingIterator) Value() *EntryListing {
	return e.value
}

func (e *entryListingIterator) Err() error {
	return e.it.Err()
}

func (e *entryListingIterator) Close() {
	e.it.Close()
}

func (e *entryListingIterator) nextNoDelimiter() bool {
	hasNext := e.it.Next()
	if !hasNext {
		e.value = nil
		return false
	}
	v := e.it.Value()
	e.value = &EntryListing{Path: v.Path, Entry: v.Entry}
	return true
}

func (e *entryListingIterator) nextWithDelimiter() bool {
	if e.value != nil && e.value.CommonPrefix {
		nextPath := e.value.Path
		if upperBound := graveler.UpperBoundForPrefix([]byte(nextPath)); upperBound != nil {
			nextPath = Path(upperBound)
		}
		e.it.SeekGE(nextPath)
	}
	if !e.it.Next() {
		e.value = nil
		return false
	}
	v := e.it.Value()
	relevantPath := v.Path[len(e.prefix):]
	delimiterIndex := strings.Index(relevantPath.String(), e.delimiter)
	if delimiterIndex == -1 {
		// listing for non common prefix with value
		e.value = &EntryListing{Path: v.Path, Entry: v.Entry}
	} else {
		// listing for common prefix key
		commonPrefixLen := len(e.prefix) + delimiterIndex + len(e.delimiter)
		commonPrefixKey := v.Path[:commonPrefixLen]
		e.value = &EntryListing{CommonPrefix: true, Path: commonPrefixKey}
	}
	return true
}
