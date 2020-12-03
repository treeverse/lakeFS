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

	sut := File{
		fh: fh,
		store: func(string) error {
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
	require.NoError(t, sut.Store(filename))

	require.True(t, storeCalled)
}

func TestWriteValidate(t *testing.T) {
	filename := uuid.Must(uuid.NewRandom()).String()
	filepath := path.Join("/tmp", filename)
	defer os.Remove(filepath)

	fh, err := os.Create(filepath)
	if err != nil {
		panic(err)
	}

	storeCalled := false

	sut := File{
		fh: fh,
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
	require.NoError(t, sut.Store("validfilename"))
	require.Error(t, sut.Store("validfilename"))
}

func TestPyramidReadFile(t *testing.T) {
	filename := uuid.Must(uuid.NewRandom()).String()
	filepath := path.Join("/tmp", filename)
	content := "some content to write to file"
	if err := ioutil.WriteFile(filepath, []byte(content), os.ModePerm); err != nil {
		panic(err)
	}
	defer os.Remove(filepath)

	mockEv := newMockEviction()

	fh, err := os.Open(filepath)
	if err != nil {
		panic(err)
	}

	sut := ROFile{
		fh:       fh,
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

	require.Equal(t, 1, mockEv.touchedTimes[relativePath(filename)])
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

func (me *mockEviction) store(_ relativePath, _ int64) int {
	return 0
}
