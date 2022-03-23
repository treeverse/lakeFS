package nessie

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/cmd/lakectl/cmd/store"
)

// Currently, this test accesses the following bucket, and so AWS should be configured to allow it
const IngestTestBucketPath = "s3://nessie-system-testing-data/ingest-test-data/"

func TestS3Walk(t *testing.T) {
	// Test bucket was uploaded with 2100 as the test is written. If the test fails on this number,
	// make sure there were no changes made to the bucket, or update this number accordingly
	const expectedNumObjs = 2100
	numObjs := 0

	store.Walk(context.Background(), IngestTestBucketPath, func(e store.ObjectStoreEntry) error {
		numObjs++
		return nil
	})

	require.Equal(t, expectedNumObjs, numObjs, "Wrong number of objects detected by Walk function")
}
