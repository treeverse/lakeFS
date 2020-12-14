package sstable

import (
	"crypto/sha256"
	"strconv"
	"testing"

	"github.com/treeverse/lakefs/graveler"

	"github.com/stretchr/testify/require"

	"github.com/golang/mock/gomock"
	"github.com/treeverse/lakefs/graveler/committed"
	"github.com/treeverse/lakefs/pyramid/mock"
)

var (
	ns = committed.Namespace("some-namespace")
)

func TestWriterFirst(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockFS := mock.NewMockFS(ctrl)

	defer ctrl.Finish()

	mockFile := mock.NewMockStoredFile(ctrl)
	mockFile.EXPECT().Close().Return(nil)

	mockFS.EXPECT().Create(string(ns)).Return(mockFile, nil)

	dw, err := newDiskWriter(mockFS, ns, sha256.New())
	require.NoError(t, err)
	require.NotNil(t, dw)

	writes := 5
	mockFile.EXPECT().Write(gomock.Any()).Return(10, nil).Times(writes)
	for i := 0; i < writes; i++ {
		err = dw.WriteRecord(graveler.ValueRecord{
			Key: []byte("some-key" + strconv.Itoa(i)),
			Value: &graveler.Value{
				Identity: []byte("some-identity" + strconv.Itoa(i)),
				Data:     []byte("some-data" + strconv.Itoa(i)),
			},
		})
		require.NoError(t, err)
	}
}
