package sstable_test

import (
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/treeverse/lakefs/graveler/committed"
	"github.com/treeverse/lakefs/graveler/sstable"

	"github.com/golang/mock/gomock"
	ssMock "github.com/treeverse/lakefs/graveler/sstable/mock"
	fsMock "github.com/treeverse/lakefs/pyramid/mock"
)

func TestGetEntrySuccess(t *testing.T) {
	ctrl := gomock.NewController(t)

	mockCache := ssMock.NewMockcache(ctrl)
	mockFS := fsMock.NewMockFS(ctrl)

	sut := sstable.NewPebbleSSTableManager(mockCache, mockFS, sha256.New())

	ns := "some-ns"
	keys := randomStrings(10)
	vals := randomStrings(len(keys))
	sstableID := "some-id"

	reader := createSStableReader(t, keys, vals)

	derefCount := 0
	mockCache.EXPECT().GetOrOpen(ns, committed.ID(sstableID)).Times(1).
		Return(reader,
			func() error {
				derefCount++
				return nil
			}, nil)

	val, err := sut.GetValue(committed.Namespace(ns), committed.Key(keys[len(keys)/3]), committed.ID(sstableID))
	require.NoError(t, err)
	require.NotNil(t, val)

	require.Equal(t, 1, derefCount)
}

func TestGetEntryCacheFailure(t *testing.T) {
	ctrl := gomock.NewController(t)

	mockCache := ssMock.NewMockcache(ctrl)
	mockFS := fsMock.NewMockFS(ctrl)

	sut := sstable.NewPebbleSSTableManager(mockCache, mockFS, sha256.New())

	ns := "some-ns"
	sstableID := "some-id"

	expectedErr := errors.New("cache failure")
	mockCache.EXPECT().GetOrOpen(ns, committed.ID(sstableID)).Times(1).
		Return(nil, nil, expectedErr)

	val, err := sut.GetValue(committed.Namespace(ns), committed.Key("some-key"), committed.ID(sstableID))
	require.Error(t, expectedErr, err)
	require.Nil(t, val)
}

func TestGetEntryNotFound(t *testing.T) {
	ctrl := gomock.NewController(t)

	mockCache := ssMock.NewMockcache(ctrl)
	mockFS := fsMock.NewMockFS(ctrl)

	sut := sstable.NewPebbleSSTableManager(mockCache, mockFS, sha256.New())

	ns := "some-ns"
	keys := randomStrings(10)
	vals := randomStrings(len(keys))
	sstableID := "some-id"

	reader := createSStableReader(t, keys, vals)

	derefCount := 0
	mockCache.EXPECT().GetOrOpen(ns, committed.ID(sstableID)).Times(1).
		Return(reader,
			func() error {
				derefCount++
				return nil
			}, nil)

	val, err := sut.GetValue(committed.Namespace(ns), committed.Key("does-not-exist"), committed.ID(sstableID))
	require.Error(t, err)
	require.Nil(t, val)

	require.Equal(t, 1, derefCount)
}

func TestGetWriterSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)

	mockCache := ssMock.NewMockcache(ctrl)
	mockFS := fsMock.NewMockFS(ctrl)

	sut := sstable.NewPebbleSSTableManager(mockCache, mockFS, sha256.New())

	ns := "some-ns"
	mockFile := fsMock.NewMockStoredFile(ctrl)
	mockFS.EXPECT().Create(ns).Return(mockFile, nil).Times(1)

	writer, err := sut.GetWriter(committed.Namespace(ns))
	require.NoError(t, err)
	require.NotNil(t, writer)

	require.IsType(t, &sstable.DiskWriter{}, writer)
	dw := writer.(*sstable.DiskWriter)

	require.Equal(t, mockFS, dw.GetFS())
	require.Equal(t, mockFile, dw.GetStoredFile())
}

func TestNewPartIteratorSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)

	mockCache := ssMock.NewMockcache(ctrl)
	mockFS := fsMock.NewMockFS(ctrl)

	sut := sstable.NewPebbleSSTableManager(mockCache, mockFS, sha256.New())

	ns := "some-ns"
	keys := randomStrings(10)
	vals := randomStrings(len(keys))
	sstableID := "some-id"

	reader := createSStableReader(t, keys, vals)
	derefCount := 0
	mockCache.EXPECT().GetOrOpen(ns, committed.ID(sstableID)).Times(1).
		Return(reader,
			func() error {
				derefCount++
				return nil
			}, nil)

	iter, err := sut.NewRangeIterator(committed.Namespace(ns), committed.ID(sstableID), committed.Key(keys[len(keys)/3]))
	require.NoError(t, err)
	require.NotNil(t, iter)

	iter.Close()
	require.NoError(t, iter.Err())

	require.Equal(t, 1, derefCount)
}
