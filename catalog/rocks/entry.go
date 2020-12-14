package rocks

import (
	"crypto/sha256"

	"github.com/treeverse/lakefs/graveler"
	"google.golang.org/protobuf/proto"
)

var TombstoneValue graveler.Value

func IsTombstoneValue(value graveler.Value) bool {
	return len(value.Identity) == 0 && len(value.Data) == 0
}

func ValueToEntry(value graveler.Value) (*Entry, error) {
	if IsTombstoneValue(value) {
		return nil, nil
	}
	var ent Entry
	err := proto.Unmarshal(value.Data, &ent)
	if err != nil {
		return nil, err
	}
	return &ent, nil
}

func EntryToValue(entry *Entry) (graveler.Value, error) {
	var value graveler.Value
	// return empty value if tombstone
	if entry == nil {
		return value, nil
	}
	// marshal data using pb
	data, err := proto.Marshal(entry)
	if err != nil {
		return value, err
	}
	// calculate checksum based on serialized data
	checksum := sha256.Sum256(data)

	value.Data = data
	value.Identity = checksum[:]
	return value, nil
}
