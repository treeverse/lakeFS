package s3

import (
	"context"
	"fmt"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/go-openapi/swag"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/stats"
)

type (
	clientFactory  func(awsSession *session.Session, cfgs ...*aws.Config) s3iface.S3API
	s3RegionGetter func(ctx context.Context, sess *session.Session, bucket string) (string, error)
)

type ClientCache struct {
	regionToS3Client sync.Map
	bucketToRegion   sync.Map
	awsSession       *session.Session

	clientFactory  clientFactory
	s3RegionGetter s3RegionGetter
	collector      stats.Collector
}

func getBucketRegionFromS3(ctx context.Context, sess *session.Session, bucket string) (string, error) {
	return s3manager.GetBucketRegion(ctx, sess, bucket, "")
}

func getBucketRegionFromSession(ctx context.Context, sess *session.Session, bucket string) (string, error) {
	region := aws.StringValue(sess.Config.Region)
	return region, nil
}

func newS3Client(sess *session.Session, cfgs ...*aws.Config) s3iface.S3API {
	return s3.New(sess, cfgs...)
}

func NewClientCache(awsSession *session.Session) *ClientCache {
	return &ClientCache{
		awsSession:     awsSession,
		clientFactory:  newS3Client,
		s3RegionGetter: getBucketRegionFromS3,
	}
}

func (c *ClientCache) SetClientFactory(clientFactory clientFactory) {
	c.clientFactory = clientFactory
}

func (c *ClientCache) SetS3RegionGetter(s3RegionGetter s3RegionGetter) {
	c.s3RegionGetter = s3RegionGetter
}

func (c *ClientCache) SetStatsCollector(statsCollector stats.Collector) {
	c.collector = statsCollector
}

func (c *ClientCache) getBucketRegion(ctx context.Context, bucket string) string {
	if region, hasRegion := c.bucketToRegion.Load(bucket); hasRegion {
		return region.(string)
	}
	logging.FromContext(ctx).WithField("bucket", bucket).Debug("requesting region for bucket")
	region, err := c.s3RegionGetter(ctx, c.awsSession, bucket)
	if err != nil {
		logging.FromContext(ctx).WithError(err).Error("failed to get region for bucket, falling back to default region")
		region = *c.awsSession.Config.Region
	}
	c.bucketToRegion.Store(bucket, region)
	return region
}

// Get returns an AWS client configured to the region of the given bucket.
func (c *ClientCache) Get(ctx context.Context, bucket string) s3iface.S3API {
	region := c.getBucketRegion(ctx, bucket)
	svc, hasClient := c.regionToS3Client.Load(region)
	if !hasClient {
		logging.FromContext(ctx).WithField("bucket", bucket).WithField("region", region).Debug("creating client for region")
		svc := c.clientFactory(c.awsSession, &aws.Config{Region: swag.String(region)})
		c.regionToS3Client.Store(region, svc)
		if c.collector != nil {
			c.collector.CollectEvent(stats.Event{
				Class: "s3_block_adapter",
				Name:  fmt.Sprintf("created_aws_client_%s", region),
			})
		}
		return svc
	}
	return svc.(s3iface.S3API)
}

func (c *ClientCache) DiscoverBucketRegion(b bool) {
	if b {
		c.s3RegionGetter = getBucketRegionFromS3
	} else {
		c.s3RegionGetter = getBucketRegionFromSession
	}
}
