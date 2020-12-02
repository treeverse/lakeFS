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
	filename := uuid.Must(uuid.NewRandom()).String()
	filepath := path.Join("/tmp", filename)
	defer os.Remove(filepath)

	fh, err := os.Create(filepath)
	if err != nil {
		panic(err)
	}

	storeCalled := false
	mockEv := newMockEviction()

	sut := File{
		fh:       fh,
		eviction: mockEv,
		readOnly: false,
		rPath:    relativePath(filename),
		store: func() error {
			storeCalled = true
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

	require.Equal(t, 0, mockEv.touchedTimes[relativePath(filename)])
	require.True(t, storeCalled)
}

func TestPyramidReadFile(t *testing.T) {
	filename := uuid.Must(uuid.NewRandom()).String()
	filepath := path.Join("/tmp", filename)
	content := "some content to write to file"
	if err := ioutil.WriteFile(filepath,[]byte(content), os.ModePerm); err != nil{
		panic(err)
	}
	defer os.Remove(filepath)

	mockEv := newMockEviction()

	fh, err := os.Open(filepath)
	if err != nil{
		panic(err)
	}

	sut := File{
		fh:       fh,
		eviction: mockEv,
		readOnly: true,
		rPath:    relativePath(filename),
		store: nil,
	}

	// no writes to read only file
	n, err := sut.Write([]byte(content))
	require.Equal(t, 0, n)
	require.Error(t, err)
	require.Equal(t, ErrReadOnlyFile, err)

	require.NoError(t, sut.Sync())
	_, err = sut.Stat()
	require.NoError(t, err)

	bytes := make([]byte, len(content))
	n, err = sut.Read(bytes)
	require.NoError(t, err)
	require.Equal(t, len(content), n)
	require.Equal(t, content, string(bytes))
	require.NoError(t, sut.Close())

	require.Equal(t, 1, mockEv.touchedTimes[relativePath(filename)])
}

type mockEviction struct {
	touchedTimes   map[relativePath]int
}

func newMockEviction() *mockEviction {
	return &mockEviction{
		touchedTimes:   map[relativePath]int{},
	}
}

func (me *mockEviction) touch(rPath relativePath) {
	me.touchedTimes[rPath]++
}

func (me *mockEviction) store(_ relativePath, _ int64) bool {
	return true
}
