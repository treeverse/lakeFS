package s3

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/go-openapi/swag"
	"github.com/treeverse/lakefs/pkg/logging"
)

type clientFactory func(awsSession client.ConfigProvider, cfgs ...*aws.Config) s3iface.S3API

type ClientCache struct {
	regionToS3Client map[string]s3iface.S3API
	bucketToRegion   map[string]string
	awsSession       *session.Session
	defaultClient    s3iface.S3API
	clientFactory    func(awsSession client.ConfigProvider, cfgs ...*aws.Config) s3iface.S3API
}

func newS3Client(p client.ConfigProvider, cfgs ...*aws.Config) s3iface.S3API {
	return s3.New(p, cfgs...)
}

func NewClientCache(awsSession *session.Session) *ClientCache {
	return &ClientCache{
		regionToS3Client: make(map[string]s3iface.S3API),
		bucketToRegion:   make(map[string]string),
		awsSession:       awsSession,
		clientFactory:    newS3Client,
		defaultClient:    newS3Client(awsSession),
	}
}

func (c *ClientCache) WithClientFactory(clientFactory clientFactory) *ClientCache {
	c.clientFactory = clientFactory
	c.defaultClient = clientFactory(c.awsSession)
	return c
}

func (c *ClientCache) getBucketRegion(ctx context.Context, bucket string) string {
	if region, hasRegion := c.bucketToRegion[bucket]; hasRegion {
		return region
	}
	logging.FromContext(ctx).WithField("bucket", bucket).Debug("requesting region for bucket")
	region, err := s3manager.GetBucketRegionWithClient(ctx, c.defaultClient, bucket)
	if err != nil {
		logging.FromContext(ctx).WithError(err).Error("failed to get region for bucket, falling back to default region")
		region = *c.awsSession.Config.Region
	}
	c.bucketToRegion[bucket] = region
	return region
}

func (c *ClientCache) Get(ctx context.Context, bucket string) s3iface.S3API {
	region := c.getBucketRegion(ctx, bucket)
	if _, hasClient := c.regionToS3Client[region]; !hasClient {
		logging.FromContext(ctx).WithField("bucket", bucket).WithField("region", region).Debug("creating client for region")
		c.regionToS3Client[region] = c.clientFactory(c.awsSession, &aws.Config{Region: swag.String(region)})
	}
	return c.regionToS3Client[region]
}
