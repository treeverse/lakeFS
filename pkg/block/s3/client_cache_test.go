package s3_test

import (
	"context"
	"errors"
	"testing"

	"github.com/go-test/deep"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	s3 "github.com/treeverse/lakefs/pkg/block/s3"
	"github.com/treeverse/lakefs/pkg/testutil"
)

var regionError = errors.New("failed to get region")

type s3ClientMock struct {
	s3iface.S3API
}

func TestCache(t *testing.T) {
	defaultRegion := "us-west-2"
	sess, err := session.NewSession(&aws.Config{Region: &defaultRegion})
	testutil.Must(t, err)
	s3Mock := &s3ClientMock{}
	tests := map[string]struct {
		bucketToRegion      map[string]string
		calls               []string
		regionErrorsIndexes map[int]bool
	}{
		"two_buckets_two_regions": {
			bucketToRegion: map[string]string{"us-bucket": "us-east-1", "eu-bucket": "eu-west-1"},
			calls:          []string{"us-bucket", "us-bucket", "us-bucket", "eu-bucket", "eu-bucket", "eu-bucket"},
		},
		"multiple_buckets_two_regions": {
			bucketToRegion: map[string]string{"us-bucket-1": "us-east-1", "us-bucket-2": "us-east-1", "us-bucket-3": "us-east-1", "eu-bucket-1": "eu-west-1", "eu-bucket-2": "eu-west-1"},
			calls:          []string{"us-bucket-1", "us-bucket-2", "us-bucket-3", "eu-bucket-1", "eu-bucket-2"},
		},
		"error_on_get_region": {
			bucketToRegion:      map[string]string{"us-bucket": "us-east-1", "eu-bucket": "eu-west-1"},
			calls:               []string{"us-bucket", "us-bucket", "us-bucket", "eu-bucket", "eu-bucket", "eu-bucket"},
			regionErrorsIndexes: map[int]bool{3: true},
		},
	}
	for name, tst := range tests {
		t.Run(name, func(t *testing.T) {
			actualClientCacheMissCount := make(map[string]int)
			actualRegionCacheMissCount := make(map[string]int)
			expectedClientCacheMissCount := make(map[string]int)
			expectedRegionCacheMissCount := make(map[string]int)

			c := s3.NewClientCache(sess)
			var callIdx int
			var call string
			c = c.WithClientFactory(func(sess *session.Session, cfgs ...*aws.Config) s3iface.S3API {
				region := sess.Config.Region
				for _, cfg := range cfgs {
					if cfg.Region != nil {
						region = cfg.Region
					}
				}
				actualClientCacheMissCount[*region]++
				return s3Mock
			})
			c = c.WithS3RegionGetter(func(ctx context.Context, sess *session.Session, bucket string) (string, error) {
				actualRegionCacheMissCount[bucket]++
				if tst.regionErrorsIndexes[callIdx] {
					return "", regionError
				}
				return tst.bucketToRegion[bucket], nil
			})
			bucketsInCache := make(map[string]bool)
			for callIdx, call = range tst.calls {
				expectedRegionCacheMissCount[call] = 1
				if _, ok := bucketsInCache[call]; !ok {
					if tst.regionErrorsIndexes[callIdx] {
						expectedClientCacheMissCount[defaultRegion] = 1
					} else {
						expectedClientCacheMissCount[tst.bucketToRegion[call]] = 1
					}
				}
				bucketsInCache[call] = true
				c.Get(context.Background(), call)
			}
			if diff := deep.Equal(expectedClientCacheMissCount, actualClientCacheMissCount); diff != nil {
				t.Fatal("unexpected client cache miss count. diff: ", diff)
			}
			if diff := deep.Equal(expectedRegionCacheMissCount, actualRegionCacheMissCount); diff != nil {
				t.Fatal("unexpected region cache miss count. diff: ", diff)
			}
		})
	}
}
