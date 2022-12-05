package graveler

import "bytes"

type filterPrefixIterator struct {
	iter   ValueIterator
	prefix Key
}

func NewFilterPrefixIterator(iter ValueIterator, prefix Key) *filterPrefixIterator {
	iter.SeekGE(prefix)
	return &filterPrefixIterator{
		iter:   iter,
		prefix: prefix,
	}
}

func (f *filterPrefixIterator) Next() bool {
	if ok := f.iter.Next(); !ok {
		return false
	}
	value := f.iter.Value()
	if !bytes.HasPrefix(value.Key, f.prefix) {
		return false
	}
	return true
}

func (f *filterPrefixIterator) SeekGE(id Key) {
	f.iter.SeekGE(id)
}

func (f *filterPrefixIterator) Value() *ValueRecord {
	return f.iter.Value()
}

func (f *filterPrefixIterator) Err() error {
	return f.iter.Err()
}

func (f *filterPrefixIterator) Close() {
	f.iter.Close()
}
