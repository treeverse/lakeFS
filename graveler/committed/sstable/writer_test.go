package sstable

import (
	"crypto/sha256"
	"sort"
	"testing"

	"github.com/thanhpk/randstr"

	"github.com/stretchr/testify/require"

	"github.com/golang/mock/gomock"
	"github.com/treeverse/lakefs/graveler/committed"
	"github.com/treeverse/lakefs/pyramid/mock"
)

func TestWriter(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockFS := mock.NewMockFS(ctrl)
	defer ctrl.Finish()
	ns := committed.Namespace("some-namespace")

	// create the mock file with the matching file-system
	mockFile := mock.NewMockStoredFile(ctrl)
	mockFile.EXPECT().Close().Return(nil).Times(1)
	mockFS.EXPECT().Create(string(ns)).Return(mockFile, nil)

	writes := 500
	dw, err := newDiskWriter(mockFS, ns, sha256.New())
	require.NoError(t, err)
	require.NotNil(t, dw)

	keys := randomStrings(writes)
	var f string

	// expect the specific write file actions
	mockFile.EXPECT().Write(gomock.Any()).DoAndReturn(
		func(b []byte) (int, error) {
			return len(b), nil
		}).MinTimes(1)
	mockFile.EXPECT().Sync().Return(nil).AnyTimes()
	mockFile.EXPECT().Store(gomock.Any()).DoAndReturn(
		func(filename string) error {
			f = filename
			return nil
		}).AnyTimes()

	// Do the actual writing
	for i := 0; i < writes; i++ {
		err = dw.WriteRecord(committed.Record{
			Key:   []byte(keys[i]),
			Value: []byte("some-data"),
		})
		require.NoError(t, err)
	}

	// Close and assert the result
	wr, err := dw.Close()
	require.NoError(t, err)
	require.NotNil(t, wr)
	require.Equal(t, writes, wr.Count)
	require.Equal(t, keys[0], string(wr.First))
	require.Equal(t, keys[writes-1], string(wr.Last))
	require.Equal(t, committed.ID(f), wr.PartID)
}

func TestWriterAbort(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockFS := mock.NewMockFS(ctrl)
	defer ctrl.Finish()
	ns := committed.Namespace("some-namespace")

	// create the mock file with the matching file-system
	mockFile := mock.NewMockStoredFile(ctrl)
	mockFile.EXPECT().Abort().Return(nil).Times(1)
	mockFile.EXPECT().Close().Return(nil).Times(1)
	mockFS.EXPECT().Create(string(ns)).Return(mockFile, nil)

	dw, err := newDiskWriter(mockFS, ns, sha256.New())
	require.NoError(t, err)
	require.NotNil(t, dw)

	// expect the specific write file actions
	mockFile.EXPECT().Write(gomock.Any()).DoAndReturn(
		func(b []byte) (int, error) {
			return len(b), nil
		}).Times(1)
	mockFile.EXPECT().Sync().Return(nil).AnyTimes()

	// Do the actual writing
	err = dw.WriteRecord(committed.Record{
		Key:   []byte("key-1"),
		Value: []byte("some-data"),
	})

	// Abort
	require.NoError(t, dw.Abort())
}

func randomStrings(writes int) []string {
	var keys []string
	for i := 0; i < writes; i++ {
		keys = append(keys, randstr.String(20, "abcdefghijklmnopqrstuvwyz0123456789"))
	}
	sort.Strings(keys)
	return keys
}
