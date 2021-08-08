package s3

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/go-openapi/swag"
	"github.com/treeverse/lakefs/pkg/logging"
)

type ClientCache struct {
	regionToS3Client map[string]s3iface.S3API
	bucketToRegion   map[string]string
	awsSession       *session.Session
}

func NewClientCache(awsSession *session.Session) *ClientCache {
	return &ClientCache{
		regionToS3Client: make(map[string]s3iface.S3API),
		bucketToRegion:   make(map[string]string),
		awsSession:       awsSession,
	}
}

func (c *ClientCache) getBucketRegion(ctx context.Context, bucket string) string {
	if region, hasRegion := c.bucketToRegion[bucket]; hasRegion {
		return region
	}
	logging.FromContext(ctx).WithField("bucket", bucket).Debug("requesting region for bucket")
	region, err := s3manager.GetBucketRegion(ctx, c.awsSession, bucket, "")
	if err != nil {
		logging.FromContext(ctx).WithError(err).Error("failed to get region for bucket, falling back to default region")
		region = *c.awsSession.Config.Region
	}
	c.bucketToRegion[bucket] = region
	return region
}

func (c *ClientCache) get(ctx context.Context, bucket string) s3iface.S3API {
	region := c.getBucketRegion(ctx, bucket)
	if _, hasClient := c.regionToS3Client[region]; !hasClient {
		logging.FromContext(ctx).WithField("bucket", bucket).WithField("region", region).Debug("creating client for region")
		c.regionToS3Client[region] = s3.New(c.awsSession, &aws.Config{Region: swag.String(region)})
	}
	return c.regionToS3Client[region]
}
