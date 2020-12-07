package sstable

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/thanhpk/randstr"

	"github.com/treeverse/lakefs/graveler"
)

func TestSerializeDeserialize(t *testing.T) {
	lenIdentity := []int{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024}
	lenData := []int{1, 2, 64, 512, 1024, 8192, 65536, 1024 * 1024}
	for _, lenI := range lenIdentity {
		for _, lenD := range lenData {
			test(t, graveler.Value{
				Identity: randstr.Bytes(lenI),
				Data:     randstr.Bytes(lenD),
			})
		}
	}
}

func TestDeserializeFailure(t *testing.T) {
	res, err := deserializeValue([]byte("this is not ok"))
	require.Nil(t, res)
	require.Error(t, err)

	// writing just the uvarint
	buf := make([]byte, 3)
	written := binary.PutUvarint(buf, 255)
	res, err = deserializeValue(buf[:written])
	require.Nil(t, res)
	require.Error(t, err)
}

func test(t *testing.T, val graveler.Value) {
	bytes, err := serializeValue(val)
	require.NoError(t, err)

	res, err := deserializeValue(bytes)
	require.NoError(t, err)

	require.Equal(t, val, *res)
}
