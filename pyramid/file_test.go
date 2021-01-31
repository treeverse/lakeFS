package pyramid

import (
	"github.com/treeverse/lakefs/pyramid/params"

	"context"
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/google/uuid"
)

func TestPyramidWriteFile(t *testing.T) {
	ctx := context.Background()
	filename := uuid.New().String()

	fh, err := ioutil.TempFile("", filename)
	if err != nil {
		t.Fatal("Failed to create temp file", err)
	}

	filepath := fh.Name()
	defer os.Remove(filepath)

	storeCalled := false
	abortCalled := false
	var storeCtx context.Context
	sut := WRFile{
		File: fh,
		store: func(innerCtx context.Context, _ string) error {
			storeCalled = true
			storeCtx = innerCtx
			return nil
		},
		abort: func(context.Context) error {
			abortCalled = true
			return nil
		},
	}

	content := "some content to write to file"
	n, err := sut.Write([]byte(content))
	require.Equal(t, len(content), n)
	require.NoError(t, err)
	require.NoError(t, sut.Sync())

	_, err = sut.Stat()
	require.NoError(t, err)

	require.NoError(t, sut.Close())
	require.NoError(t, sut.Store(ctx, filename))
	require.NotNil(t, sut.cancelStore)

	require.True(t, storeCalled)

	require.Error(t, sut.Abort(ctx))
	require.False(t, abortCalled)
	require.Nil(t, sut.cancelStore)
	require.Equal(t, storeCtx.Err(), context.Canceled)
}

func TestWriteValidate(t *testing.T) {
	ctx := context.Background()
	filename := uuid.New().String()
	fh, err := ioutil.TempFile("", filename)
	if err != nil {
		t.Fatal("Failed to create temp file", err)
	}

	filepath := fh.Name()
	defer os.Remove(filepath)

	storeCalled := false

	sut := WRFile{
		File: fh,
		store: func(context.Context, string) error {
			storeCalled = true
			return nil
		},
	}

	content := "some content to write to file"
	n, err := sut.Write([]byte(content))
	require.Equal(t, len(content), n)
	require.NoError(t, err)

	require.NoError(t, sut.Close())
	require.Error(t, sut.Store(ctx, "workspace"+string(os.PathSeparator)))
	require.False(t, storeCalled)

	require.Error(t, sut.Close())
}

func TestMultipleWriteCalls(t *testing.T) {
	ctx := context.Background()
	filename := uuid.New().String()
	fh, err := ioutil.TempFile("", filename)
	if err != nil {
		t.Fatal("Failed to create temp file", err)
	}

	filepath := fh.Name()
	defer os.Remove(filepath)

	storeCalled := false

	sut := WRFile{
		File: fh,
		store: func(context.Context, string) error {
			storeCalled = true
			return nil
		},
	}

	content := "some content to write to file"
	n, err := sut.Write([]byte(content))
	require.Equal(t, len(content), n)
	require.NoError(t, err)

	require.NoError(t, sut.Close())
	require.NoError(t, sut.Store(ctx, "validfilename"))
	require.True(t, storeCalled)

	require.Error(t, sut.Store(ctx, "validfilename"))
}

func TestAbort(t *testing.T) {
	ctx := context.Background()
	filename := uuid.New().String()
	fh, err := ioutil.TempFile("", filename)
	if err != nil {
		t.Fatal("Failed to create temp file", err)
	}

	filepath := fh.Name()
	defer os.Remove(filepath)

	storeCalled := false
	abortCalled := false

	sut := WRFile{
		File: fh,
		store: func(context.Context, string) error {
			storeCalled = true
			return nil
		},
		abort: func(context.Context) error {
			abortCalled = true
			return nil
		},
	}

	content := "some content to write to file"
	n, err := sut.Write([]byte(content))
	require.Equal(t, len(content), n)
	require.NoError(t, err)

	require.NoError(t, sut.Close())
	require.False(t, abortCalled)

	require.NoError(t, sut.Abort(ctx))
	require.False(t, storeCalled)
	require.True(t, abortCalled)

	require.Error(t, sut.Store(ctx, "validfilename"))
	require.False(t, storeCalled)
}

func TestPyramidReadFile(t *testing.T) {
	filename := uuid.New().String()
	filepath := path.Join("/tmp", filename)
	content := "some content to write to file"
	if err := ioutil.WriteFile(filepath, []byte(content), os.ModePerm); err != nil {
		t.Fatalf("Failed to write file %s: %s", filepath, err)
	}
	defer os.Remove(filepath)

	mockEv := newMockEviction()

	fh, err := os.Open(filepath)
	if err != nil {
		t.Fatalf("Failed to open file %s: %s", filepath, err)
	}

	sut := ROFile{
		File:     fh,
		eviction: mockEv,
		rPath:    params.RelativePath(filename),
	}

	_, err = sut.Stat()
	require.NoError(t, err)

	bytes := make([]byte, len(content))
	n, err := sut.Read(bytes)
	require.NoError(t, err)
	require.Equal(t, len(content), n)
	require.Equal(t, content, string(bytes))
	require.NoError(t, sut.Close())

	require.Equal(t, 2, mockEv.touchedTimes[params.RelativePath(filename)])
}

type mockEviction struct {
	touchedTimes map[params.RelativePath]int
}

func newMockEviction() *mockEviction {
	return &mockEviction{
		touchedTimes: map[params.RelativePath]int{},
	}
}

func (me *mockEviction) Touch(rPath params.RelativePath) {
	me.touchedTimes[rPath]++
}

func (me *mockEviction) Store(_ params.RelativePath, _ int64) bool {
	return false
}
