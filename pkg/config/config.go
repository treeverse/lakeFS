package config

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/mitchellh/go-homedir"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	authparams "github.com/treeverse/lakefs/pkg/auth/params"
	"github.com/treeverse/lakefs/pkg/block/factory"
	blockparams "github.com/treeverse/lakefs/pkg/block/params"
	dbparams "github.com/treeverse/lakefs/pkg/db/params"
	"github.com/treeverse/lakefs/pkg/graveler/committed"
	"github.com/treeverse/lakefs/pkg/logging"
	pyramidparams "github.com/treeverse/lakefs/pkg/pyramid/params"
)

const (
	DefaultBlockStoreType                    = "local"
	DefaultBlockStoreLocalPath               = "~/data/lakefs/block"
	DefaultBlockStoreS3Region                = "us-east-1"
	DefaultBlockStoreS3StreamingChunkSize    = 2 << 19         // 1MiB by default per chunk
	DefaultBlockStoreS3StreamingChunkTimeout = time.Second * 1 // or 1 seconds, whatever comes first

	DefaultCommittedLocalCacheRangePercent          = 0.9
	DefaultCommittedLocalCacheMetaRangePercent      = 0.1
	DefaultCommittedLocalCacheBytes                 = 1 * 1024 * 1024 * 1024
	DefaultCommittedLocalCacheDir                   = "~/data/lakefs/cache"
	DefaultCommittedPebbleSSTableCacheSizeBytes     = 400_000_000
	DefaultCommittedLocalCacheNumUploaders          = 10
	DefaultCommittedBlockStoragePrefix              = "_lakefs"
	DefaultCommittedPermanentMinRangeSizeBytes      = 0
	DefaultCommittedPermanentMaxRangeSizeBytes      = 20 * 1024 * 1024
	DefaultCommittedPermanentRangeRaggednessEntries = 50_000

	DefaultBlockStoreGSS3Endpoint = "https://storage.googleapis.com"

	DefaultAuthCacheEnabled = true
	DefaultAuthCacheSize    = 1024
	DefaultAuthCacheTTL     = 20 * time.Second
	DefaultAuthCacheJitter  = 3 * time.Second

	DefaultListenAddr          = "0.0.0.0:8000"
	DefaultS3GatewayDomainName = "s3.local.lakefs.io"
	DefaultS3GatewayRegion     = "us-east-1"
	DefaultS3MaxRetries        = 5

	DefaultStatsEnabled       = true
	DefaultStatsAddr          = "https://stats.treeverse.io"
	DefaultStatsFlushInterval = time.Second * 30

	DefaultAzureTryTimeout = 10 * time.Minute
	DefaultAzureAuthMethod = "access-key"
)

var (
	ErrBadConfiguration  = errors.New("bad configuration")
	ErrMissingSecretKey  = fmt.Errorf("%w: auth.encrypt.secret_key cannot be empty", ErrBadConfiguration)
	ErrInvalidProportion = fmt.Errorf("%w: total proportion isn't 1.0", ErrBadConfiguration)
)

type LogrusAWSAdapter struct {
	logger *log.Entry
}

func (l *LogrusAWSAdapter) Log(vars ...interface{}) {
	l.logger.Debug(vars...)
}

type Config struct {
	values configuration
}

func NewConfig() (*Config, error) {
	c := &Config{}

	// Inform viper of all expected fields.  Otherwise it fails to deserialize from the
	// environment.
	keys := GetStructKeys(reflect.TypeOf(c.values), "mapstructure", "squash")
	for _, key := range keys {
		viper.SetDefault(key, nil)
	}

	setDefaults()
	setupLogger()

	err := viper.UnmarshalExact(&c.values)
	return c, err
}

// Default flag keys
const (
	ListenAddressKey = "listen_address"

	LoggingFormatKey = "logging.format"
	LoggingLevelKey  = "logging.level"
	LoggingOutputKey = "logging.output"

	AuthCacheEnabledKey = "auth.cache.enabled"
	AuthCacheSizeKey    = "auth.cache.size"
	AuthCacheTTLKey     = "auth.cache.ttl"
	AuthCacheJitterKey  = "auth.cache.jitter"

	BlockstoreTypeKey                    = "blockstore.type"
	BlockstoreLocalPathKey               = "blockstore.local.path"
	BlockstoreS3RegionKey                = "blockstore.s3.region"
	BlockstoreS3StreamingChunkSizeKey    = "blockstore.s3.streaming_chunk_size"
	BlockstoreS3StreamingChunkTimeoutKey = "blockstore.s3.streaming_chunk_timeout"
	BlockstoreS3MaxRetriesKey            = "blockstore.s3.max_retries"

	BlockstoreAzureTryTimeoutKey                = "blockstore.azure.try_timeout"
	BlockstoreAzureStorageAccountKey            = "blockstore.azure.storage_account"
	BlockstoreAzureStorageAccessKey             = "blockstore.azure.storage_access_key"
	BlockstoreAzureAuthMethod                   = "blockstore.azure.auth_method"
	CommittedLocalCacheSizeBytesKey             = "committed.local_cache.size_bytes"
	CommittedLocalCacheDirKey                   = "committed.local_cache.dir"
	CommittedLocalCacheNumUploadersKey          = "committed.local_cache.max_uploaders_per_writer"
	CommittedLocalCacheRangeProportionKey       = "committed.local_cache.range_proportion"
	CommittedLocalCacheMetaRangeProportionKey   = "committed.local_cache.metarange_proportion"
	CommittedBlockStoragePrefixKey              = "committed.block_storage_prefix"
	CommittedPermanentStorageMinRangeSizeKey    = "committed.permanent.min_range_size_bytes"
	CommittedPermanentStorageMaxRangeSizeKey    = "committed.permanent.max_range_size_bytes"
	CommittedPermanentStorageRangeRaggednessKey = "committed.permanent.range_raggedness_entries"

	CommittedPebbleSSTableCacheSizeBytesKey = "committed.sstable.memory.cache_size_bytes"

	GatewaysS3DomainNameKey = "gateways.s3.domain_name"
	GatewaysS3RegionKey     = "gateways.s3.region"

	BlockstoreGSS3EndpointKey = "blockstore.gs.s3_endpoint"

	StatsEnabledKey       = "stats.enabled"
	StatsAddressKey       = "stats.address"
	StatsFlushIntervalKey = "stats.flush_interval"
)

func setDefaults() {
	viper.SetDefault(ListenAddressKey, DefaultListenAddr)

	viper.SetDefault(LoggingFormatKey, DefaultLoggingFormat)
	viper.SetDefault(LoggingLevelKey, DefaultLoggingLevel)
	viper.SetDefault(LoggingOutputKey, DefaultLoggingOutput)

	viper.SetDefault(AuthCacheEnabledKey, DefaultAuthCacheEnabled)
	viper.SetDefault(AuthCacheSizeKey, DefaultAuthCacheSize)
	viper.SetDefault(AuthCacheTTLKey, DefaultAuthCacheTTL)
	viper.SetDefault(AuthCacheJitterKey, DefaultAuthCacheJitter)

	viper.SetDefault(BlockstoreTypeKey, DefaultBlockStoreType)
	viper.SetDefault(BlockstoreLocalPathKey, DefaultBlockStoreLocalPath)
	viper.SetDefault(BlockstoreS3RegionKey, DefaultBlockStoreS3Region)
	viper.SetDefault(BlockstoreS3StreamingChunkSizeKey, DefaultBlockStoreS3StreamingChunkSize)
	viper.SetDefault(BlockstoreS3StreamingChunkTimeoutKey, DefaultBlockStoreS3StreamingChunkTimeout)
	viper.SetDefault(BlockstoreS3MaxRetriesKey, DefaultS3MaxRetries)

	viper.SetDefault(CommittedLocalCacheSizeBytesKey, DefaultCommittedLocalCacheBytes)
	viper.SetDefault(CommittedLocalCacheDirKey, DefaultCommittedLocalCacheDir)
	viper.SetDefault(CommittedLocalCacheNumUploadersKey, DefaultCommittedLocalCacheNumUploaders)
	viper.SetDefault(CommittedLocalCacheRangeProportionKey, DefaultCommittedLocalCacheRangePercent)
	viper.SetDefault(CommittedLocalCacheMetaRangeProportionKey, DefaultCommittedLocalCacheMetaRangePercent)

	viper.SetDefault(CommittedBlockStoragePrefixKey, DefaultCommittedBlockStoragePrefix)
	viper.SetDefault(CommittedPermanentStorageMinRangeSizeKey, DefaultCommittedPermanentMinRangeSizeBytes)
	viper.SetDefault(CommittedPermanentStorageMaxRangeSizeKey, DefaultCommittedPermanentMaxRangeSizeBytes)
	viper.SetDefault(CommittedPermanentStorageRangeRaggednessKey, DefaultCommittedPermanentRangeRaggednessEntries)
	viper.SetDefault(CommittedPebbleSSTableCacheSizeBytesKey, DefaultCommittedPebbleSSTableCacheSizeBytes)

	viper.SetDefault(GatewaysS3DomainNameKey, DefaultS3GatewayDomainName)
	viper.SetDefault(GatewaysS3RegionKey, DefaultS3GatewayRegion)

	viper.SetDefault(BlockstoreGSS3EndpointKey, DefaultBlockStoreGSS3Endpoint)

	viper.SetDefault(StatsEnabledKey, DefaultStatsEnabled)
	viper.SetDefault(StatsAddressKey, DefaultStatsAddr)
	viper.SetDefault(StatsFlushIntervalKey, DefaultStatsFlushInterval)

	viper.SetDefault(BlockstoreAzureTryTimeoutKey, DefaultAzureTryTimeout)
	viper.SetDefault(BlockstoreAzureAuthMethod, DefaultAzureAuthMethod)
}

func (c *Config) GetDatabaseParams() dbparams.Database {
	return dbparams.Database{
		ConnectionString:      c.values.Database.ConnectionString,
		MaxOpenConnections:    c.values.Database.MaxOpenConnections,
		MaxIdleConnections:    c.values.Database.MaxIdleConnections,
		ConnectionMaxLifetime: c.values.Database.ConnectionMaxLifetime,
	}
}

func (c *Config) GetAwsConfig() *aws.Config {
	cfg := &aws.Config{
		Region: aws.String(c.values.Blockstore.S3.Region),
		Logger: &LogrusAWSAdapter{log.WithField("sdk", "aws")},
	}
	level := strings.ToLower(logging.Level())
	if level == "trace" {
		cfg.LogLevel = aws.LogLevel(aws.LogDebugWithRequestRetries | aws.LogDebugWithRequestErrors)
	}
	if c.values.Blockstore.S3.Profile != "" || c.values.Blockstore.S3.CredentialsFile != "" {
		cfg.Credentials = credentials.NewSharedCredentials(
			c.values.Blockstore.S3.CredentialsFile,
			c.values.Blockstore.S3.Profile,
		)
	}
	if c.values.Blockstore.S3.Credentials != nil {
		secretAccessKey := c.values.Blockstore.S3.Credentials.SecretAccessKey
		if secretAccessKey == "" {
			logging.Default().Warn("blockstore.s3.credentials.access_secret_key is deprecated. Use instead: blockstore.s3.credentials.secret_access_key.")
			secretAccessKey = c.values.Blockstore.S3.Credentials.AccessSecretKey
		}
		cfg.Credentials = credentials.NewStaticCredentials(
			c.values.Blockstore.S3.Credentials.AccessKeyID,
			secretAccessKey,
			c.values.Blockstore.S3.Credentials.SessionToken,
		)
	}

	s3Endpoint := c.values.Blockstore.S3.Endpoint
	if len(s3Endpoint) > 0 {
		cfg = cfg.WithEndpoint(s3Endpoint)
	}
	s3ForcePathStyle := c.values.Blockstore.S3.ForcePathStyle
	if s3ForcePathStyle {
		cfg = cfg.WithS3ForcePathStyle(true)
	}
	cfg = cfg.WithMaxRetries(c.values.Blockstore.S3.MaxRetries)
	return cfg
}

func (c *Config) GetBlockstoreType() string {
	return c.values.Blockstore.Type
}

func (c *Config) GetBlockAdapterS3Params() (blockparams.S3, error) {
	cfg := c.GetAwsConfig()

	return blockparams.S3{
		AwsConfig:             cfg,
		StreamingChunkSize:    c.values.Blockstore.S3.StreamingChunkSize,
		StreamingChunkTimeout: c.values.Blockstore.S3.StreamingChunkTimeout,
	}, nil
}

func (c *Config) GetBlockAdapterLocalParams() (blockparams.Local, error) {
	localPath := c.values.Blockstore.Local.Path
	path, err := homedir.Expand(localPath)
	if err != nil {
		return blockparams.Local{}, fmt.Errorf("parse blockstore location URI %s: %w", localPath, err)
	}

	return blockparams.Local{Path: path}, nil
}

func (c *Config) GetBlockAdapterGSParams() (blockparams.GS, error) {
	return blockparams.GS{
		CredentialsFile: c.values.Blockstore.GS.CredentialsFile,
		CredentialsJSON: c.values.Blockstore.GS.CredentialsJSON,
	}, nil
}

func (c *Config) GetBlockAdapterAzureParams() (blockparams.Azure, error) {
	return blockparams.Azure{
		StorageAccount:   c.values.Blockstore.Azure.StorageAccount,
		StorageAccessKey: c.values.Blockstore.Azure.StorageAccessKey,
		AuthMethod:       c.values.Blockstore.Azure.AuthMethod,
		TryTimeout:       c.values.Blockstore.Azure.TryTimeout,
	}, nil
}

func (c *Config) GetAuthCacheConfig() authparams.ServiceCache {
	return authparams.ServiceCache{
		Enabled:        c.values.Auth.Cache.Enabled,
		Size:           c.values.Auth.Cache.Size,
		TTL:            c.values.Auth.Cache.TTL,
		EvictionJitter: c.values.Auth.Cache.Jitter,
	}
}

func (c *Config) GetAuthEncryptionSecret() []byte {
	secret := c.values.Auth.Encrypt.SecretKey
	if len(secret) == 0 {
		panic(fmt.Errorf("%w. Please set it to a unique, randomly generated value and store it somewhere safe", ErrMissingSecretKey))
	}
	return []byte(secret)
}

func (c *Config) GetS3GatewayRegion() string {
	return c.values.Gateways.S3.Region
}

func (c *Config) GetS3GatewayDomainName() string {
	return c.values.Gateways.S3.DomainName
}

func (c *Config) GetS3GatewayFallbackURL() string {
	return c.values.Gateways.S3.FallbackURL
}

func (c *Config) GetListenAddress() string {
	return c.values.ListenAddress
}

func (c *Config) GetStatsEnabled() bool {
	return c.values.Stats.Enabled
}

func (c *Config) GetStatsAddress() string {
	return c.values.Stats.Address
}

func (c *Config) GetStatsFlushInterval() time.Duration {
	return c.values.Stats.FlushInterval
}

const floatSumTolerance = 1e-6

// GetCommittedTierFSParams returns parameters for building a tierFS.  Caller must separately
// build and populate Adapter.
func (c *Config) GetCommittedTierFSParams(ctx context.Context) (*pyramidparams.ExtParams, error) {
	adapter, err := factory.BuildBlockAdapter(ctx, c)
	if err != nil {
		return nil, fmt.Errorf("build block adapter: %w", err)
	}
	rangePro := c.values.Committed.LocalCache.RangeProportion
	metaRangePro := c.values.Committed.LocalCache.MetaRangeProportion

	if math.Abs(rangePro+metaRangePro-1) > floatSumTolerance {
		return nil, fmt.Errorf("range_proportion(%f) and metarange_proportion(%f): %w", rangePro, metaRangePro, ErrInvalidProportion)
	}

	localCacheDir, err := homedir.Expand(c.values.Committed.LocalCache.Dir)
	if err != nil {
		return nil, fmt.Errorf("expand %s: %w", c.values.Committed.LocalCache.Dir, err)
	}

	logger := logging.Default().WithField("module", "pyramid")
	return &pyramidparams.ExtParams{
		RangeAllocationProportion:     rangePro,
		MetaRangeAllocationProportion: metaRangePro,
		SharedParams: pyramidparams.SharedParams{
			Logger:             logger,
			Adapter:            adapter,
			BlockStoragePrefix: c.values.Committed.BlockStoragePrefix,
			Local: pyramidparams.LocalDiskParams{
				BaseDir:             localCacheDir,
				TotalAllocatedBytes: c.values.Committed.LocalCache.SizeBytes,
			},
			PebbleSSTableCacheSizeBytes: c.values.Committed.SSTable.Memory.CacheSizeBytes,
		},
	}, nil
}

func (c *Config) GetCommittedParams() *committed.Params {
	return &committed.Params{
		MinRangeSizeBytes:          c.values.Committed.Permanent.MinRangeSizeBytes,
		MaxRangeSizeBytes:          c.values.Committed.Permanent.MaxRangeSizeBytes,
		RangeSizeEntriesRaggedness: c.values.Committed.Permanent.RangeRaggednessEntries,
		MaxUploaders:               c.values.Committed.LocalCache.MaxUploadersPerWriter,
	}
}

func (c *Config) GetFixedInstallationID() string {
	return c.values.Installation.FixedID
}
