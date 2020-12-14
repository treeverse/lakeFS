package sstable

import (
	"crypto/sha256"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pyramid/mock"
)

func TestMain(m *testing.M) {
	m.Run()
}

func TestWriterFirst(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockFS := mock.NewMockFS(ctrl)

	_, err := newDiskWriter(mockFS, "namespace", sha256.New())

	require.NoError(t, err)
	mockFS.EXPECT().Create("")

}
