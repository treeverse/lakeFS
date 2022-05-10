package export_test

import (
	"context"
	_ "embed"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/kv/export"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
	_ "github.com/treeverse/lakefs/pkg/kv/mem"
	"github.com/treeverse/lakefs/pkg/logging"
)

func TestImport(t *testing.T) {
	ctx := context.Background()
	logger := logging.Default()
	store := kvtest.MakeStoreByName("mem", "")(t, context.Background())
	defer store.Close()

	err := export.Import(ctx, store, logger, "test_data/test.tar.gz")
	require.NoError(t, err)

	itr, err := store.Scan(ctx, []byte("1"))
	defer itr.Close()
	count := 0
	for itr.Next() {
		count++
		e := itr.Entry()
		key := strconv.Itoa(count)
		value := key + ". I am a test file"
		require.Equal(t, key, string(e.Key))
		require.Equal(t, value, string(e.Value))
	}
	require.Equal(t, 4, count)
}

func TestImportNoFile(t *testing.T) {
	ctx := context.Background()
	logger := logging.Default()
	store := kvtest.MakeStoreByName("mem", "")(t, context.Background())
	defer store.Close()

	err := export.Import(ctx, store, logger, "bad_path.tar.gz")
	require.ErrorIs(t, err, os.ErrNotExist)
}
