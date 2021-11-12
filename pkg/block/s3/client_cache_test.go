package s3_test

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/go-test/deep"
	"github.com/treeverse/lakefs/pkg/block/s3"
	"github.com/treeverse/lakefs/pkg/testutil"
)

var regionError = errors.New("failed to get region")

func TestClientCache(t *testing.T) {
	defaultRegion := "us-west-2"
	sess, err := session.NewSession(&aws.Config{Region: &defaultRegion})
	testutil.Must(t, err)
	tests := map[string]struct {
		bucketToRegion      map[string]string
		bucketCalls         []string
		regionErrorsIndexes map[int]bool
	}{
		"two_buckets_two_regions": {
			bucketToRegion: map[string]string{"us-bucket": "us-east-1", "eu-bucket": "eu-west-1"},
			bucketCalls:    []string{"us-bucket", "us-bucket", "us-bucket", "eu-bucket", "eu-bucket", "eu-bucket"},
		},
		"multiple_buckets_two_regions": {
			bucketToRegion: map[string]string{"us-bucket-1": "us-east-1", "us-bucket-2": "us-east-1", "us-bucket-3": "us-east-1", "eu-bucket-1": "eu-west-1", "eu-bucket-2": "eu-west-1"},
			bucketCalls:    []string{"us-bucket-1", "us-bucket-2", "us-bucket-3", "eu-bucket-1", "eu-bucket-2"},
		},
		"error_on_get_region": {
			bucketToRegion:      map[string]string{"us-bucket": "us-east-1", "eu-bucket": "eu-west-1"},
			bucketCalls:         []string{"us-bucket", "us-bucket", "us-bucket", "eu-bucket", "eu-bucket", "eu-bucket"},
			regionErrorsIndexes: map[int]bool{3: true},
		},
		"all_errors": {
			bucketToRegion:      map[string]string{"us-bucket-1": "us-east-1", "us-bucket-2": "us-east-1", "us-bucket-3": "us-east-1", "eu-bucket-1": "eu-west-1", "eu-bucket-2": "eu-west-1"},
			bucketCalls:         []string{"us-bucket-1", "us-bucket-2", "us-bucket-3", "eu-bucket-1", "eu-bucket-2"},
			regionErrorsIndexes: map[int]bool{0: true, 1: true, 2: true, 3: true, 4: true},
		},
		"alternating_regions": {
			bucketToRegion: map[string]string{"us-bucket-1": "us-east-1", "us-bucket-2": "us-east-1", "us-bucket-3": "us-east-1", "eu-bucket-1": "eu-west-1", "eu-bucket-2": "eu-west-1"},
			bucketCalls:    []string{"us-bucket-1", "eu-bucket-1", "us-bucket-2", "eu-bucket-2", "us-bucket-3", "us-bucket-1", "eu-bucket-1", "us-bucket-2", "eu-bucket-2", "us-bucket-3"},
		},
	}
	for name, tst := range tests {
		t.Run(name, func(t *testing.T) {
			var callIdx int
			var bucket string
			actualClientsCreated := make(map[string]bool)
			expectedClientsCreated := make(map[string]bool)
			actualRegionFetch := make(map[string]bool)
			expectedRegionFetch := make(map[string]bool)
			c := s3.NewClientCache(sess)

			c.SetClientFactory(func(sess *session.Session, cfgs ...*aws.Config) s3iface.S3API {
				region := sess.Config.Region
				for _, cfg := range cfgs {
					if cfg.Region != nil {
						region = cfg.Region
					}
				}
				if actualClientsCreated[*region] {
					t.Fatalf("client created more than once for a region")
				}
				actualClientsCreated[*region] = true
				return struct{ s3iface.S3API }{}
			})
			c.SetS3RegionGetter(func(ctx context.Context, sess *session.Session, bucket string) (string, error) {
				if actualRegionFetch[bucket] {
					t.Fatalf("region fetched more than once for bucket")
				}
				actualRegionFetch[bucket] = true
				if tst.regionErrorsIndexes[callIdx] {
					return "", regionError
				}
				return tst.bucketToRegion[bucket], nil
			})
			alreadyCalled := make(map[string]bool)
			for callIdx, bucket = range tst.bucketCalls {
				expectedRegionFetch[bucket] = true // for every bucket, there should be exactly 1 region fetch
				if _, ok := alreadyCalled[bucket]; !ok {
					if tst.regionErrorsIndexes[callIdx] {
						// if there's an error, a client should be created for the default region
						expectedClientsCreated[defaultRegion] = true
					} else {
						// for every region, a client is created exactly once
						expectedClientsCreated[tst.bucketToRegion[bucket]] = true
					}
				}
				alreadyCalled[bucket] = true
				c.Get(context.Background(), bucket)
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
