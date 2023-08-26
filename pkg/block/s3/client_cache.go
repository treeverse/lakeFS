package s3

import (
	"context"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/treeverse/lakefs/pkg/block/params"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/stats"
)

type (
	clientFactory  func(region string) *s3.Client
	s3RegionGetter func(ctx context.Context, bucket string) (string, error)
)

type ClientCache struct {
	regionToS3Client sync.Map
	bucketToRegion   sync.Map
	awsConfig        aws.Config
	defaultClient    *s3.Client
	clientFactory    clientFactory
	s3RegionGetter   s3RegionGetter
	collector        stats.Collector
}

func NewClientCache(awsConfig aws.Config, params params.S3) *ClientCache {
	clientFactory := newClientFactory(awsConfig, WithClientParams(params))
	defaultClient := clientFactory(awsConfig.Region)
	clientCache := &ClientCache{
		awsConfig:     awsConfig,
		defaultClient: defaultClient,
		clientFactory: clientFactory,
		collector:     &stats.NullCollector{},
	}
	clientCache.DiscoverBucketRegion(true)
	return clientCache
}

// newClientFactory returns a function that creates a new S3 client with the given region.
// accepts aws configuration and list of s3 options functions to apply on the client.
// the factory function is used to create a new client for a region when it is not cached.
func newClientFactory(awsConfig aws.Config, s3OptFns ...func(options *s3.Options)) clientFactory {
	return func(region string) *s3.Client {
		return s3.NewFromConfig(awsConfig, func(options *s3.Options) {
			for _, opts := range s3OptFns {
				opts(options)
			}
			options.Region = region
		})
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

func (c *ClientCache) bucketRegion(ctx context.Context, bucket string) string {
	if region, hasRegion := c.bucketToRegion.Load(bucket); hasRegion {
		return region.(string)
	}
	region, err := c.s3RegionGetter(ctx, bucket)
	if err != nil {
		region = c.awsConfig.Region
		logging.FromContext(ctx).
			WithError(err).
			WithField("default_region", region).
			Error("Failed to get region for bucket, falling back to default region")
	}
	c.bucketToRegion.Store(bucket, region)
	return region
}

func (c *ClientCache) DiscoverBucketRegion(b bool) {
	if b {
		c.s3RegionGetter = c.getBucketRegionFromAWS
	} else {
		c.s3RegionGetter = c.getBucketRegionDefault
	}
}

func (c *ClientCache) getBucketRegionFromAWS(ctx context.Context, bucket string) (string, error) {
	return manager.GetBucketRegion(ctx, c.defaultClient, bucket)
}

func (c *ClientCache) getBucketRegionDefault(_ context.Context, _ string) (string, error) {
	return c.awsConfig.Region, nil
}

func (c *ClientCache) Get(ctx context.Context, bucket string) *s3.Client {
	region := c.bucketRegion(ctx, bucket)
	if client, found := c.regionToS3Client.Load(region); found {
		return client.(*s3.Client)
	}

	logging.FromContext(ctx).WithField("bucket", bucket).WithField("region", region).Debug("creating client for region")
	client := c.clientFactory(region)
	c.regionToS3Client.Store(region, client)
	c.collector.CollectEvent(stats.Event{
		Class: "s3_block_adapter",
		Name:  "created_aws_client_" + region,
	})
	return client
}

func (c *ClientCache) GetDefault() *s3.Client {
	return c.defaultClient
}
