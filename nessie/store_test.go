package nessie

import (
	"context"
	"net/url"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/cmd/lakectl/cmd/store"
)

const IngestTestBucketPath = "s3://nessie-system-testing-data/ingest-test-data/"

func TestS3Walk(t *testing.T) {
	s3Client, err := store.GetS3Client(*aws.NewConfig().WithRegion("us-east-1"))
	require.NoError(t, err, "Failed to create S3 client")
	walker := &store.S3Walker{S3: s3Client}

	// Test bucket was uploaded with 2100 as the test is written. If the test fails on this number,
	// make sure there were no changes made to the bucket, or update this number accordingly
	const expectedNumObjs = 2100
	numObjs := 0

	uri, err := url.Parse(IngestTestBucketPath)
	require.NoError(t, err, "url parsing failed", IngestTestBucketPath)

	walker.Walk(context.Background(), uri, func(e store.ObjectStoreEntry) error {
		numObjs++
		return nil
	})

	require.Equal(t, expectedNumObjs, numObjs, "Wrong number of objects detected by Walk function")
}
