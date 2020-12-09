package graveler

import "bytes"

// ListingIter implements a listing iterator using a ValueIterator
// assumes all values in valueIterator start with prefix
type ListingIter struct {
	valueIterator ValueIterator
	delimiter     Key
	nextDelimiter Key
	prefix        Key
	current       *Listing
	nextFunc      func(l *ListingIter) bool
	err           error
}

// getFollowingValue returns the following value (i.e will increase the last byte by 1)
// in the following cases will return received value : value is nil, value length is 0, last byte is maximum byte
func getFollowingValue(value []byte) []byte {
	if len(value) == 0 || value[len(value)-1] == 255 {
		return value
	}
	copiedDelimiter := make([]byte, len(value))
	copy(copiedDelimiter, value)
	return append(copiedDelimiter[:len(copiedDelimiter)-1], copiedDelimiter[len(copiedDelimiter)-1]+1)
}

func NewListingIter(iterator ValueIterator, delimiter, prefix Key) *ListingIter {
	var nextDelimiter Key
	var nextFunc func(l *ListingIter) bool
	if len(delimiter) == 0 {
		nextFunc = nextNoDelimiter
	} else {
		nextFunc = nextWithDelimiter
		nextDelimiter = getFollowingValue(delimiter)
	}

	return &ListingIter{
		valueIterator: iterator,
		delimiter:     delimiter,
		nextDelimiter: nextDelimiter,
		prefix:        prefix,
		nextFunc:      nextFunc,
	}
}

func nextNoDelimiter(l *ListingIter) bool {
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

func nextWithDelimiter(l *ListingIter) bool {
	var hasNext bool
	if l.current == nil || !l.current.CommonPrefix {
		hasNext = l.valueIterator.Next()
	} else {
		nextKey := append(l.current.Key, l.nextDelimiter...)
		l.valueIterator.SeekGE(nextKey)
		hasNext = l.valueIterator.Next()
	}

	if hasNext {
		nextValue := l.valueIterator.Value()
		if !bytes.HasPrefix(nextValue.Key, l.prefix) {
			l.current = nil
			l.err = ErrUnexpected
			return false
		}
		l.current = l.getListingFromValue(nextValue.Value, nextValue.Key)
	} else {
		l.current = nil
	}
	return hasNext
}

func (l *ListingIter) Next() bool {
	return l.nextFunc(l)
}

func (l *ListingIter) getListingFromValue(value *Value, key Key) *Listing {
	relevantPath := key[len(l.prefix):]
	delimiterIndex := bytes.Index(relevantPath, l.delimiter)
	commonPrefix := delimiterIndex >= 0
	if commonPrefix {
		relevantPath = relevantPath[:delimiterIndex]
		value = nil
	}
	newKey := append(l.prefix, relevantPath...)
	return &Listing{
		Key:          newKey,
		CommonPrefix: commonPrefix,
		Value:        value,
	}
}

func (l *ListingIter) SeekGE(id Key) {
	l.current = nil
	l.valueIterator.SeekGE(id)
}

func (l *ListingIter) Value() *Listing {
	return l.current
}

func (l *ListingIter) Err() error {
	if l.err != nil {
		return l.err
	}
	return l.valueIterator.Err()
}

func (l *ListingIter) Close() {
	l.valueIterator.Close()
}
