package s3

import (
	"context"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/go-openapi/swag"
	"github.com/treeverse/lakefs/pkg/logging"
)

type clientFactory func(awsSession *session.Session, cfgs ...*aws.Config) s3iface.S3API
type s3RegionGetter func(ctx context.Context, sess *session.Session, bucket string) (string, error)

type ClientCache struct {
	regionToS3Client sync.Map
	bucketToRegion   sync.Map
	awsSession       *session.Session

	clientFactory  clientFactory
	s3RegionGetter s3RegionGetter
}

func getBucketRegionFromS3(ctx context.Context, sess *session.Session, bucket string) (string, error) {
	return s3manager.GetBucketRegion(ctx, sess, bucket, "")
}

func newS3Client(sess *session.Session, cfgs ...*aws.Config) s3iface.S3API {
	return s3.New(sess, cfgs...)
}

func NewClientCache(awsSession *session.Session) *ClientCache {
	cc := &ClientCache{
		awsSession:     awsSession,
		clientFactory:  newS3Client,
		s3RegionGetter: getBucketRegionFromS3,
	}
	return cc
}

func (c *ClientCache) WithClientFactory(clientFactory clientFactory) *ClientCache {
	c.clientFactory = clientFactory
	return c
}

func (c *ClientCache) WithS3RegionGetter(s3RegionGetter s3RegionGetter) *ClientCache {
	c.s3RegionGetter = s3RegionGetter
	return c
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
	if _, hasClient := c.regionToS3Client.Load(region); !hasClient {
		logging.FromContext(ctx).WithField("bucket", bucket).WithField("region", region).Debug("creating client for region")
		svc := c.clientFactory(c.awsSession, &aws.Config{Region: swag.String(region)})
		c.regionToS3Client.Store(region, svc)
		return svc
	}
	svc, _ := c.regionToS3Client.Load(region)
	return svc.(s3iface.S3API)
}
