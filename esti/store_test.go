package esti

import (
	"context"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/ingest/store"
)

func testImportBucketPath(t *testing.T) string {
	t.Helper()
	blockstoreType := viper.GetString(config.BlockstoreTypeKey)
	switch blockstoreType {
	case block.BlockstoreTypeS3:
		return "s3://esti-system-testing-data/ingest-test-data/"
	case block.BlockstoreTypeGS:
		return "gs://esti-system-testing-data/"
	case block.BlockstoreTypeAzure:
		return "https://esti.blob.core.windows.net/esti-system-testing-data/"
	default:
		t.Skipf("No import bucket path for '%s' blockstore", blockstoreType)
	}
	return ""
}

func TestWalk(t *testing.T) {
	SkipTestIfAskedTo(t)
	storageURI := testImportBucketPath(t)

	// Test bucket was uploaded with 2100 as the test is written. If the test fails on this number,
	// make sure there were no changes made to the bucket, or update this number accordingly
	const expectedNumObjs = 2100
	numObjs := 0

	walker, err := store.NewFactory(nil).GetWalker(context.Background(), store.WalkerOptions{
		S3EndpointURL: "",
		StorageURI:    storageURI,
	})
	require.NoError(t, err)
	err = walker.Walk(context.Background(), store.WalkOptions{}, func(e store.ObjectStoreEntry) error {
		numObjs++
		return nil
	})

	require.NoError(t, err)
	require.Equal(t, expectedNumObjs, numObjs, "Wrong number of objects detected by Walk function")
}
