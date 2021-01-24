package sstable_test

import (
	"io/ioutil"
	"os"
	"sort"
	"testing"

	"github.com/cockroachdb/pebble"
	pebblesst "github.com/cockroachdb/pebble/sstable"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/graveler/committed"
	"github.com/treeverse/lakefs/graveler/sstable"
)

func TestIteratorSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	count := 1000
	keys := randomStrings(count)
	sort.Strings(keys)
	vals := randomStrings(count)
	iter := createSStableIterator(t, keys, vals)

	called := 0
	sut := sstable.NewIterator(iter, func() error {
		called++
		return nil
	})
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
func createSStableIterator(t *testing.T, keys, vals []string) pebblesst.Iterator {
	ssReader := createSStableReader(t, keys, vals)

	iter, err := ssReader.NewIter(nil, nil)
	require.NoError(t, err)

	t.Cleanup(func() {
		iter.Close()
	})

	return iter
}

// createSStableReader creates the table from keys, vals passed to it
func createSStableReader(t *testing.T, keys []string, vals []string) *pebblesst.Reader {
	f, err := ioutil.TempFile(os.TempDir(), "test file")
	require.NoError(t, err)
	w := pebblesst.NewWriter(f, pebblesst.WriterOptions{
		Compression: pebblesst.SnappyCompression,
	})
	for i, key := range keys {
		require.NoError(t, w.Set([]byte(key), []byte(vals[i])))
	}
	require.NoError(t, w.Close())

	cache := pebble.NewCache(0)
	t.Cleanup(func() {
		cache.Unref()
	})

	readF, err := os.Open(f.Name())
	require.NoError(t, err)
	ssReader, err := pebblesst.NewReader(readF, pebblesst.ReaderOptions{Cache: cache})
	require.NoError(t, err)

	t.Cleanup(func() {
		ssReader.Close()
	})
	return ssReader
}
