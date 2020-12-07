package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/treeverse/lakefs/graveler"
)

func serializeValue(inVal graveler.Value) ([]byte, error) {
	buf := make([]byte, 3)
	written := binary.PutUvarint(buf, uint64(len(inVal.Identity)))

	res := bytes.NewBuffer(buf[:written])
	if _, err := res.Write(inVal.Identity); err != nil {
		return nil, err
	}
	if _, err := res.Write(inVal.Data); err != nil {
		return nil, err
	}

	return res.Bytes(), nil
}

func deserializeValue(bytes []byte) (*graveler.Value, error) {
	idLen, readBytes := binary.Uvarint(bytes)
	if readBytes < 0 || len(bytes) < readBytes+int(idLen) {
		return nil, errors.New("invalid graveler value")
	}

	return &graveler.Value{
		Identity: bytes[readBytes : readBytes+int(idLen)],
		Data:     bytes[readBytes+int(idLen):],
	}, nil
}
