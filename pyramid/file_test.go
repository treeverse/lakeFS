package pyramid

import (
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/google/uuid"
)

func TestPyramidWriteFile(t *testing.T) {
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
		store: func(string) error {
			storeCalled = true
			return nil
		},
		abort: func() error {
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
	require.NoError(t, sut.Store(filename))

	require.True(t, storeCalled)

	require.Error(t, sut.Abort())
	require.False(t, abortCalled)
}

func TestWriteValidate(t *testing.T) {
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
		store: func(string) error {
			storeCalled = true
			return nil
		},
	}

	content := "some content to write to file"
	n, err := sut.Write([]byte(content))
	require.Equal(t, len(content), n)
	require.NoError(t, err)

	require.NoError(t, sut.Close())
	require.Error(t, sut.Store("workspace"+string(os.PathSeparator)))
	require.False(t, storeCalled)

	require.Error(t, sut.Close())
}

func TestMultipleWriteCalls(t *testing.T) {
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
		store: func(string) error {
			storeCalled = true
			return nil
		},
	}

	content := "some content to write to file"
	n, err := sut.Write([]byte(content))
	require.Equal(t, len(content), n)
	require.NoError(t, err)

	require.NoError(t, sut.Close())
	require.NoError(t, sut.Store("validfilename"))
	require.True(t, storeCalled)

	require.Error(t, sut.Store("validfilename"))
}

func TestAbort(t *testing.T) {
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
		store: func(string) error {
			storeCalled = true
			return nil
		},
		abort: func() error {
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
	require.NoError(t, sut.Abort())
	require.False(t, storeCalled)
	require.True(t, abortCalled)

	require.Error(t, sut.Store("validfilename"))
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
		rPath:    relativePath(filename),
	}

	_, err = sut.Stat()
	require.NoError(t, err)

	bytes := make([]byte, len(content))
	n, err := sut.Read(bytes)
	require.NoError(t, err)
	require.Equal(t, len(content), n)
	require.Equal(t, content, string(bytes))
	require.NoError(t, sut.Close())

	require.Equal(t, 2, mockEv.touchedTimes[relativePath(filename)])
}

type mockEviction struct {
	touchedTimes map[relativePath]int
}

func newMockEviction() *mockEviction {
	return &mockEviction{
		touchedTimes: map[relativePath]int{},
	}
}

func (me *mockEviction) touch(rPath relativePath) {
	me.touchedTimes[rPath]++
}

func (me *mockEviction) store(_ relativePath, _ int64) bool {
	return false
}
