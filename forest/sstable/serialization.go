package sstable

import (
	"github.com/golang/protobuf/proto"
	"github.com/treeverse/lakefs/graveler"
)

func serializeValue(inVal graveler.Value) ([]byte, error) {
	return proto.Marshal(&Value{
		Identity: inVal.Identity,
		Data:     inVal.Data,
	})
}

func deserializeValue(bytes []byte) (*graveler.Value, error) {
	var val Value
	if err := proto.Unmarshal(bytes, &val); err != nil {
		return nil, err
	}

	return &graveler.Value{
		Identity: val.Identity,
		Data:     val.Data,
	}, nil
}
