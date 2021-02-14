package sstable_test

import (
	"context"
	"crypto"
	"errors"
	"sort"
	"testing"

	pebblesst "github.com/cockroachdb/pebble/sstable"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/graveler/committed"
	"github.com/treeverse/lakefs/graveler/sstable"
	fsMock "github.com/treeverse/lakefs/pyramid/mock"
)

func makeNewReader(r fakeReader) func(context.Context, committed.Namespace, committed.ID) (*pebblesst.Reader, error) {
	return func(context.Context, committed.Namespace, committed.ID) (*pebblesst.Reader, error) {
		return r.Reader, nil
	}
}

func TestGetEntrySuccess(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)

	mockFS := fsMock.NewMockFS(ctrl)

	keys := randomStrings(10)
	sort.Strings(keys)
	vals := randomStrings(len(keys))

	reader := createSStableReader(t, keys, vals)

	sut := sstable.NewPebbleSSTableRangeManagerWithNewReader(makeNewReader(reader), mockFS, crypto.SHA256)

	ns := "some-ns"
	sstableID := "some-id"

	val, err := sut.GetValue(ctx, committed.Namespace(ns), committed.ID(sstableID), committed.Key(keys[len(keys)/3]))
	require.NoError(t, err)
	require.NotNil(t, val)

	require.Equal(t, 1, reader.GetNumClosed())
}

func TestGetEntryCacheFailure(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)

	expectedErr := errors.New("cache failure")

	mockFS := fsMock.NewMockFS(ctrl)

	sut := sstable.NewPebbleSSTableRangeManagerWithNewReader(func(context.Context, committed.Namespace, committed.ID) (*pebblesst.Reader, error) {
		return nil, expectedErr
	}, mockFS, crypto.SHA256)

	ns := "some-ns"
	sstableID := committed.ID("some-id")

	val, err := sut.GetValue(ctx, committed.Namespace(ns), committed.ID(sstableID), committed.Key("some-key"))
	require.Error(t, expectedErr, err)
	require.Nil(t, val)
}

func TestGetEntryNotFound(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)

	mockFS := fsMock.NewMockFS(ctrl)

	keys := randomStrings(10)
	sort.Strings(keys)
	vals := randomStrings(len(keys))

	reader := createSStableReader(t, keys, vals)

	sut := sstable.NewPebbleSSTableRangeManagerWithNewReader(makeNewReader(reader), mockFS, crypto.SHA256)

	ns := "some-ns"
	sstableID := committed.ID("some-id")

	val, err := sut.GetValue(ctx, committed.Namespace(ns), committed.ID(sstableID), committed.Key("does-not-exist"))
	require.Error(t, err)
	require.Nil(t, val)

	require.Equal(t, 1, reader.GetNumClosed())
}

func TestGetWriterSuccess(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)

	mockFS := fsMock.NewMockFS(ctrl)

	sut := sstable.NewPebbleSSTableRangeManagerWithNewReader(nil, mockFS, crypto.SHA256)

	ns := "some-ns"
	mockFile := fsMock.NewMockStoredFile(ctrl)
	mockFS.EXPECT().Create(ctx, ns).Return(mockFile, nil).Times(1)

	writer, err := sut.GetWriter(ctx, committed.Namespace(ns), nil)
	require.NoError(t, err)
	require.NotNil(t, writer)

	require.IsType(t, &sstable.DiskWriter{}, writer)
	dw := writer.(*sstable.DiskWriter)

	require.Equal(t, mockFS, dw.GetFS())
	require.Equal(t, mockFile, dw.GetStoredFile())
}

func TestNewPartIteratorSuccess(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)

	mockFS := fsMock.NewMockFS(ctrl)

	keys := randomStrings(10)
	sort.Strings(keys)
	vals := randomStrings(len(keys))
	reader := createSStableReader(t, keys, vals)

	sut := sstable.NewPebbleSSTableRangeManagerWithNewReader(makeNewReader(reader), mockFS, crypto.SHA256)

	ns := "some-ns"
	sstableID := committed.ID("some-id")

	iter, err := sut.NewRangeIterator(ctx, committed.Namespace(ns), committed.ID(sstableID))

	require.NoError(t, err)
	require.NotNil(t, iter)

	iter.SeekGE(committed.Key(keys[len(keys)/3]))
	require.NoError(t, iter.Err())

	iter.Close()
	require.NoError(t, iter.Err())

	require.Equal(t, 1, reader.GetNumClosed())
}

func TestGetWriterRangeID(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)

	mockFS := fsMock.NewMockFS(ctrl)

	sut := sstable.NewPebbleSSTableRangeManagerWithNewReader(nil, mockFS, crypto.SHA256)

	for times := 0; times < 2; times++ {
		const ns = "some-ns"
		mockFile := fsMock.NewMockStoredFile(ctrl)
		mockFile.EXPECT().Write(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
			return len(b), nil
		}).AnyTimes()
		mockFile.EXPECT().Sync().Return(nil).AnyTimes()
		mockFile.EXPECT().Close().Return(nil).Times(1)
		mockFile.EXPECT().Store(gomock.Any(), gomock.Any()).Return(nil).Times(1)
		mockFS.EXPECT().Create(ctx, ns).Return(mockFile, nil).Times(1)

		writer, err := sut.GetWriter(ctx, ns, nil)
		require.NoError(t, err)
		require.NotNil(t, writer)
		err = writer.WriteRecord(committed.Record{
			Key:   []byte("key"),
			Value: []byte("value"),
		})
		require.NoError(t, err)
		result, err := writer.Close()
		require.NoError(t, err)
		require.NotNil(t, result)
		expectedID := committed.ID("1f60902cb44890618d61597673a62e44cf526f5991d9db687141218985fe60b8")
		require.Equal(t, expectedID, result.RangeID, "Range ID should be kept the same based on the content")
	}
}
