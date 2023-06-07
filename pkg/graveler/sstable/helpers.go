package sstable

import "github.com/cockroachdb/pebble"

func retrieveValue(lazyValue pebble.LazyValue) ([]byte, error) {
	val, owned, err := lazyValue.Value(nil)
	if err != nil {
		return nil, err
	}
	if owned || val == nil {
		return val, nil
	}
	var copiedVal = make([]byte, len(val))
	copy(copiedVal, val)
	return copiedVal, nil
}
