package rocks

import (
	"crypto/sha256"

	"github.com/treeverse/lakefs/graveler"
	"google.golang.org/protobuf/proto"
)

func ValueToEntry(value *graveler.Value) (*Entry, error) {
	var ent Entry
	err := proto.Unmarshal(value.Data, &ent)
	if err != nil {
		return nil, err
	}
	return &ent, nil
}

func EntryToValue(entry *Entry) (*graveler.Value, error) {
	// marshal data using pb
	data, err := proto.Marshal(entry)
	if err != nil {
		return nil, err
	}
	// calculate checksum based on serialized data
	checksum := sha256.Sum256(data)
	return &graveler.Value{
		Identity: checksum[:],
		Data:     data,
	}, nil
}
