package s3

import (
	"context"
	"fmt"
	"sync"

	s3manager "github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/treeverse/lakefs/pkg/block/params"
	aws_helper "github.com/treeverse/lakefs/pkg/cloud/aws"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/stats"
)

type clientFactory func(params *params.AWSParams, opts ...func(*s3.Options)) *s3.Client
type s3RegionGetter func(ctx context.Context, params *params.AWSParams, bucket string) (string, error)

type ClientCache struct {
	regionToS3Client sync.Map
	bucketToRegion   sync.Map
	params           params.AWSParams

	clientFactory  clientFactory
	s3RegionGetter s3RegionGetter
	collector      stats.Collector
}

func getBucketRegionFromS3(ctx context.Context, params *params.AWSParams, bucket string) (string, error) {
	c := aws_helper.NewS3Client(params)
	return s3manager.GetBucketRegion(ctx, c, bucket)
}

func getBucketRegionFromParams(ctx context.Context, params *params.AWSParams, bucket string) (string, error) {
	return params.Region, nil
}

func newS3Client(params *params.AWSParams, opts ...func(*s3.Options)) *s3.Client {
	return aws_helper.NewS3Client(params, opts...)
}

func NewClientCache(params *params.AWSParams) *ClientCache {
	return &ClientCache{
		params:         *params,
		clientFactory:  newS3Client,
		s3RegionGetter: getBucketRegionFromParams,
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
	region, err := c.s3RegionGetter(ctx, &c.params, bucket)
	if err != nil {
		logging.FromContext(ctx).WithError(err).Error("failed to get region for bucket, falling back to default region")
		region = c.params.Region
	}
	c.bucketToRegion.Store(bucket, region)
	return region
}

// Get returns an AWS client configured to the region of the given bucket,
// applying additional options.
func (c *ClientCache) Get(ctx context.Context, bucket string, opts ...func(o *s3.Options)) *s3.Client {
	region := c.getBucketRegion(ctx, bucket)
	svc, hasClient := c.regionToS3Client.Load(region)
	if !hasClient {
		logging.FromContext(ctx).WithField("bucket", bucket).WithField("region", region).Debug("creating client for region")
		svc := c.clientFactory(&c.params, append(opts, func(o *s3.Options) { o.Region = region })...)
		c.regionToS3Client.Store(region, svc)
		if c.collector != nil {
			c.collector.CollectEvent("s3_block_adapter", fmt.Sprintf("created_aws_client_%s", region))
		}
		return svc
	}
	return svc.(*s3.Client)
}

func (c *ClientCache) DiscoverBucketRegion(b bool) {
	if b {
		c.s3RegionGetter = getBucketRegionFromS3
	} else {
		c.s3RegionGetter = getBucketRegionFromParams
	}
}
