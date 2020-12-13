package sstable

import (
	"github.com/treeverse/lakefs/graveler"
)

func serializeValue(inVal graveler.Value) ([]byte, error) {
	// Do nothing for now..
	return nil, nil
}

func deserializeValue(bytes []byte) (*graveler.Value, error) {
	// Do nothing for now..
	return nil, nil
}
