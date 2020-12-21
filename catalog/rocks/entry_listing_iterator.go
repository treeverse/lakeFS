package rocks

import (
	"math"
	"strings"
)

type entryListingIterator struct {
	it        EntryIterator
	prefix    string
	delimiter string
	nextFunc  func() bool
	value     *EntryListing
}

// getListingNextPath returns the following value (i.e will increase the last byte by 1)
// in the following cases will return received value: empty value, the last byte is math.MaxUint8
func getListingNextPath(value Path) Path {
	if len(value) == 0 || value[len(value)-1] == math.MaxUint8 {
		return value
	}
	copiedDelimiter := make([]byte, len(value))
	copy(copiedDelimiter, value)
	copiedDelimiter[len(copiedDelimiter)-1]++
	return Path(copiedDelimiter)
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
	e.value = &EntryListing{
		CommonPrefix: false,
		Path:         v.Path,
		Entry:        v.Entry,
	}
	return true
}

func (e *entryListingIterator) nextWithDelimiter() bool {
	if e.value != nil && e.value.CommonPrefix {
		nextPath := getListingNextPath(e.value.Path)
		e.it.SeekGE(nextPath)
	}
	hasNext := e.it.Next()
	if hasNext {
		e.value = e.newEntryListingFromRecord(e.it.Value())
	} else {
		e.value = nil
	}
	return hasNext
}

func (e *entryListingIterator) newEntryListingFromRecord(record *EntryRecord) *EntryListing {
	relevantPath := record.Path[len(e.prefix):]
	delimiterIndex := strings.Index(relevantPath.String(), e.delimiter)
	if delimiterIndex == -1 {
		// return listing for non common prefix with value
		return &EntryListing{
			CommonPrefix: false,
			Path:         record.Path,
			Entry:        record.Entry,
		}
	}
	// return listing for common prefix key
	commonPrefixLen := len(e.prefix) + delimiterIndex + len(e.delimiter)
	commonPrefixKey := record.Path[:commonPrefixLen]
	return &EntryListing{
		CommonPrefix: true,
		Path:         commonPrefixKey,
	}
}
