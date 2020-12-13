package graveler

import (
	"bytes"
	"math"
)

// listingIterator implements a ListingIterator using a ValueIterator
// assumes all values in valueIterator start with prefix
type listingIterator struct {
	valueIterator ValueIterator
	delimiter     Key
	prefix        Key
	current       *Listing
	nextFunc      func() bool
}

// getFollowingValue returns the following value (i.e will increase the last byte by 1)
// in the following cases will return received value : value is nil, value length is 0, last byte is maximum byte
func getFollowingValue(value []byte) []byte {
	if len(value) == 0 || value[len(value)-1] == math.MaxUint8 {
		return value
	}
	copiedDelimiter := make([]byte, len(value))
	copy(copiedDelimiter, value)
	copiedDelimiter[len(copiedDelimiter)-1]++
	return copiedDelimiter
}

func NewListingIterator(iterator ValueIterator, delimiter, prefix Key) ListingIterator {
	l := &listingIterator{
		valueIterator: iterator,
		delimiter:     delimiter,
		prefix:        prefix,
	}
	if len(delimiter) == 0 {
		l.nextFunc = l.nextNoDelimiter
	} else {
		l.nextFunc = l.nextWithDelimiter
	}
	return l
}

func (l *listingIterator) nextNoDelimiter() bool {
	hasNext := l.valueIterator.Next()
	if !hasNext {
		l.current = nil
		return false
	}
	val := l.valueIterator.Value()
	l.current = &Listing{
		CommonPrefix: false,
		Key:          val.Key,
		Value:        val.Value,
	}
	return true
}

func (l *listingIterator) nextWithDelimiter() bool {
	if l.current != nil && l.current.CommonPrefix {
		nextKey := getFollowingValue(l.current.Key)
		l.valueIterator.SeekGE(nextKey)
	}
	hasNext := l.valueIterator.Next()
	if hasNext {
		nextValue := l.valueIterator.Value()
		l.current = l.getListingFromValue(nextValue.Value, nextValue.Key)
	} else {
		l.current = nil
	}
	return hasNext
}

func (l *listingIterator) Next() bool {
	return l.nextFunc()
}

func (l *listingIterator) getListingFromValue(value *Value, key Key) *Listing {
	relevantKey := key[len(l.prefix):]
	delimiterIndex := bytes.Index(relevantKey, l.delimiter)
	if delimiterIndex == -1 {
		// return listing for non common prefix with value
		return &Listing{
			CommonPrefix: false,
			Key:          key,
			Value:        value,
		}
	}
	// return listing for common prefix key
	commonPrefixKey := key[:len(l.prefix)+delimiterIndex+len(l.delimiter)]
	return &Listing{
		CommonPrefix: true,
		Key:          commonPrefixKey,
	}
}

func (l *listingIterator) SeekGE(id Key) {
	l.current = nil
	l.valueIterator.SeekGE(id)
}

func (l *listingIterator) Value() *Listing {
	return l.current
}

func (l *listingIterator) Err() error {
	return l.valueIterator.Err()
}

func (l *listingIterator) Close() {
	l.valueIterator.Close()
}
