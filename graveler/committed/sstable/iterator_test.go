package sstable

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/treeverse/lakefs/graveler/committed"

	"github.com/stretchr/testify/require"

	"github.com/cockroachdb/pebble/sstable"

	"github.com/golang/mock/gomock"
)

func TestIteratorSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	count := 1000
	keys := randomStrings(count)
	vals := randomStrings(count)
	iter, releaser := createSStableIterator(t, keys, vals)
	defer releaser()

	called := 0
	sut := NewIterator(iter, func() error {
		called++
		return nil
	}, nil)
	require.NotNil(t, sut)

	// read first -> nothing to read
	require.Nil(t, sut.Value())
	require.NoError(t, sut.Err())

	// advance by one and read
	require.True(t, sut.Next())
	val := sut.Value()
	require.NoError(t, sut.Err())
	require.NotNil(t, val)
	require.Equal(t, committed.Key(keys[0]), val.Key)
	require.NotNil(t, val.Value)

	// advance by one and read
	require.True(t, sut.Next())
	val = sut.Value()
	require.NoError(t, sut.Err())
	require.NotNil(t, val)
	require.Equal(t, committed.Key(keys[1]), val.Key)
	require.NotNil(t, val.Value)

	// seek to a random offset
	seekedKeyIndex := count / 3
	seekedKey := committed.Key(keys[seekedKeyIndex])
	sut.SeekGE(seekedKey)
	require.NoError(t, sut.Err())
	// value should be nil until next is called
	require.Nil(t, sut.Value())
	require.True(t, sut.Next())
	val = sut.Value()
	require.NoError(t, sut.Err())
	require.NotNil(t, val)
	require.Equal(t, seekedKey, val.Key)
	require.NotNil(t, val.Value)

	// read till the end
	for i := seekedKeyIndex + 1; i < count; i++ {
		require.True(t, sut.Next())
		val = sut.Value()
		require.NoError(t, sut.Err())
		require.NotNil(t, val)
		require.Equal(t, committed.Key(keys[i]), val.Key)
		require.NotNil(t, val.Value)
	}

	// reached the end
	require.False(t, sut.Next())
	require.NoError(t, sut.Err())

	sut.Close()
	require.NoError(t, sut.Err())
	require.Equal(t, 1, called)
}

// createSStableIterator creates the iterator from keys, vals passed to it
func createSStableIterator(t *testing.T, keys, vals []string) (sstable.Iterator, func()) {
	ssReader, releaser := createSStableReader(t, keys, vals)

	iter, err := ssReader.NewIter(nil, nil)
	require.NoError(t, err)
	return iter, func() {
		iter.Close()
		releaser()
	}
}

// createSStableReader creates the table from keys, vals passed to it
func createSStableReader(t *testing.T, keys []string, vals []string) (*sstable.Reader, func()) {
	f, err := ioutil.TempFile(os.TempDir(), "test file")
	w := sstable.NewWriter(f, sstable.WriterOptions{
		Compression: sstable.SnappyCompression,
	})
	for i, key := range keys {
		require.NoError(t, w.Set([]byte(key), []byte(vals[i])))
	}
	require.NoError(t, w.Close())

	readF, err := os.Open(f.Name())
	require.NoError(t, err)

	ssReader, err := sstable.NewReader(readF, sstable.ReaderOptions{})
	require.NoError(t, err)

	return ssReader, func() { ssReader.Close() }
}
