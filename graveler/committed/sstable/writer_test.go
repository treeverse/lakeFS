package sstable

import (
	"crypto/sha256"
	"testing"

	"github.com/treeverse/lakefs/pyramid/mock"

	"github.com/stretchr/testify/require"
)

func TestWriterFirst(t *testing.T) {
	dw, err := newDiskWriter(&mock.FS{}, "", sha256.New())
	require.NoError(t, err)

	dw.WriteRecord()
	dw.Close()
}
