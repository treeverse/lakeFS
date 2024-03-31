package s3_test

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/config"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/go-test/deep"
	"github.com/treeverse/lakefs/pkg/block/params"
	"github.com/treeverse/lakefs/pkg/block/s3"
	"github.com/treeverse/lakefs/util/testutil"
)

var errRegion = errors.New("failed to get region")

func TestClientCache(t *testing.T) {
	const defaultRegion = "us-west-2"
	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(defaultRegion))
	testutil.Must(t, err)

	tests := []struct {
		name                string
		bucketToRegion      map[string]string
		bucketCalls         []string
		regionErrorsIndexes map[int]bool
	}{
		{
			name:           "two_buckets_two_regions",
			bucketToRegion: map[string]string{"us-bucket": "us-east-1", "eu-bucket": "eu-west-1"},
			bucketCalls:    []string{"us-bucket", "us-bucket", "us-bucket", "eu-bucket", "eu-bucket", "eu-bucket"},
		},
		{
			name:           "multiple_buckets_two_regions",
			bucketToRegion: map[string]string{"us-bucket-1": "us-east-1", "us-bucket-2": "us-east-1", "us-bucket-3": "us-east-1", "eu-bucket-1": "eu-west-1", "eu-bucket-2": "eu-west-1"},
			bucketCalls:    []string{"us-bucket-1", "us-bucket-2", "us-bucket-3", "eu-bucket-1", "eu-bucket-2"},
		},
		{
			name:                "error_on_get_region",
			bucketToRegion:      map[string]string{"us-bucket": "us-east-1", "eu-bucket": "eu-west-1"},
			bucketCalls:         []string{"us-bucket", "us-bucket", "us-bucket", "eu-bucket", "eu-bucket", "eu-bucket"},
			regionErrorsIndexes: map[int]bool{3: true},
		},
		{
			name:                "all_errors",
			bucketToRegion:      map[string]string{"us-bucket-1": "us-east-1", "us-bucket-2": "us-east-1", "us-bucket-3": "us-east-1", "eu-bucket-1": "eu-west-1", "eu-bucket-2": "eu-west-1"},
			bucketCalls:         []string{"us-bucket-1", "us-bucket-2", "us-bucket-3", "eu-bucket-1", "eu-bucket-2"},
			regionErrorsIndexes: map[int]bool{0: true, 1: true, 2: true, 3: true, 4: true},
		},
		{
			name:           "alternating_regions",
			bucketToRegion: map[string]string{"us-bucket-1": "us-east-1", "us-bucket-2": "us-east-1", "us-bucket-3": "us-east-1", "eu-bucket-1": "eu-west-1", "eu-bucket-2": "eu-west-1"},
			bucketCalls:    []string{"us-bucket-1", "eu-bucket-1", "us-bucket-2", "eu-bucket-2", "us-bucket-3", "us-bucket-1", "eu-bucket-1", "us-bucket-2", "eu-bucket-2", "us-bucket-3"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var callIdx int
			var bucket string
			actualClientsCreated := make(map[string]bool)
			expectedClientsCreated := make(map[string]bool)
			actualRegionFetch := make(map[string]bool)
			expectedRegionFetch := make(map[string]bool)

			c := s3.NewClientCache(cfg, params.S3{}) // params are ignored as we use custom client factory

			c.SetClientFactory(func(region string) *awss3.Client {
				if actualClientsCreated[region] {
					t.Fatalf("client created more than once for a region")
				}
				actualClientsCreated[region] = true
				return awss3.NewFromConfig(cfg, func(o *awss3.Options) {
					o.Region = region
				})
			})

			c.SetS3RegionGetter(func(ctx context.Context, bucket string) (string, error) {
				if actualRegionFetch[bucket] {
					t.Fatalf("region fetched more than once for bucket")
				}
				actualRegionFetch[bucket] = true
				if test.regionErrorsIndexes[callIdx] {
					return "", errRegion
				}
				return test.bucketToRegion[bucket], nil
			})

			alreadyCalled := make(map[string]bool)
			for callIdx, bucket = range test.bucketCalls {
				expectedRegionFetch[bucket] = true // for every bucket, there should be exactly one region fetch
				if _, ok := alreadyCalled[bucket]; !ok {
					if test.regionErrorsIndexes[callIdx] {
						// if there's an error, a client should be created for the default region
						expectedClientsCreated[defaultRegion] = true
					} else {
						// for every region, a client is created exactly once
						expectedClientsCreated[test.bucketToRegion[bucket]] = true
					}
				}
				alreadyCalled[bucket] = true
				c.Get(ctx, bucket)
			}
			if diff := deep.Equal(expectedClientsCreated, actualClientsCreated); diff != nil {
				t.Fatal("unexpected client creation count: ", diff)
			}
			if diff := deep.Equal(expectedRegionFetch, actualRegionFetch); diff != nil {
				t.Fatal("unexpected region fetch count. diff: ", diff)
			}
		})
	}
}
