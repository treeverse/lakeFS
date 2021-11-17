package config

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	aws_retry "github.com/aws/aws-sdk-go-v2/aws/retry"
	aws_config "github.com/aws/aws-sdk-go-v2/config"
	aws_credentials "github.com/aws/aws-sdk-go-v2/credentials"
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/mitchellh/go-homedir"
	"github.com/mitchellh/mapstructure"
	"github.com/spf13/viper"
	authparams "github.com/treeverse/lakefs/pkg/auth/params"
	"github.com/treeverse/lakefs/pkg/block"
	blockparams "github.com/treeverse/lakefs/pkg/block/params"
	dbparams "github.com/treeverse/lakefs/pkg/db/params"
	"github.com/treeverse/lakefs/pkg/graveler/committed"
	"github.com/treeverse/lakefs/pkg/logging"
	pyramidparams "github.com/treeverse/lakefs/pkg/pyramid/params"
)

const (
	DefaultBlockStoreLocalPath               = "~/data/lakefs/block"
	DefaultBlockStoreS3Region                = "us-east-1"
	DefaultBlockStoreS3StreamingChunkSize    = 2 << 19         // 1MiB by default per chunk
	DefaultBlockStoreS3StreamingChunkTimeout = time.Second * 1 // or 1 seconds, whatever comes first
	DefaultBlockStoreS3DiscoverBucketRegion  = true

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
	ErrBadConfiguration    = errors.New("bad configuration")
	ErrMissingSecretKey    = fmt.Errorf("%w: auth.encrypt.secret_key cannot be empty", ErrBadConfiguration)
	ErrInvalidProportion   = fmt.Errorf("%w: total proportion isn't 1.0", ErrBadConfiguration)
	ErrBadDomainNames      = fmt.Errorf("%w: domain names are prefixes", ErrBadConfiguration)
	ErrMissingRequiredKeys = fmt.Errorf("%w: missing required keys", ErrBadConfiguration)
)

type Config struct {
	values configuration
}

func NewConfig() (*Config, error) {
	c := &Config{}

	// Inform viper of all expected fields.  Otherwise, it fails to deserialize from the
	// environment.
	keys := GetStructKeys(reflect.TypeOf(c.values), "mapstructure", "squash")
	for _, key := range keys {
		viper.SetDefault(key, nil)
	}

	setDefaults()
	setupLogger()

	err := viper.UnmarshalExact(&c.values, viper.DecodeHook(
		mapstructure.ComposeDecodeHookFunc(
			DecodeStrings, mapstructure.StringToTimeDurationHookFunc())))
	if err != nil {
		return nil, err
	}

	err = c.validateDomainNames()
	if err != nil {
		return nil, err
	}

	return c, nil
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
	BlockstoreS3DiscoverBucketRegionKey  = "blockstore.s3.discover_bucket_region"

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

	GatewaysS3DomainNamesKey = "gateways.s3.domain_name"
	GatewaysS3RegionKey      = "gateways.s3.region"

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

	viper.SetDefault(BlockstoreLocalPathKey, DefaultBlockStoreLocalPath)
	viper.SetDefault(BlockstoreS3RegionKey, DefaultBlockStoreS3Region)
	viper.SetDefault(BlockstoreS3StreamingChunkSizeKey, DefaultBlockStoreS3StreamingChunkSize)
	viper.SetDefault(BlockstoreS3StreamingChunkTimeoutKey, DefaultBlockStoreS3StreamingChunkTimeout)
	viper.SetDefault(BlockstoreS3MaxRetriesKey, DefaultS3MaxRetries)
	viper.SetDefault(BlockstoreS3StreamingChunkSizeKey, DefaultBlockStoreS3StreamingChunkSize)
	viper.SetDefault(BlockstoreS3DiscoverBucketRegionKey, DefaultBlockStoreS3DiscoverBucketRegion)

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

	viper.SetDefault(GatewaysS3DomainNamesKey, DefaultS3GatewayDomainName)
	viper.SetDefault(GatewaysS3RegionKey, DefaultS3GatewayRegion)

	viper.SetDefault(BlockstoreGSS3EndpointKey, DefaultBlockStoreGSS3Endpoint)

	viper.SetDefault(StatsEnabledKey, DefaultStatsEnabled)
	viper.SetDefault(StatsAddressKey, DefaultStatsAddr)
	viper.SetDefault(StatsFlushIntervalKey, DefaultStatsFlushInterval)

	viper.SetDefault(BlockstoreAzureTryTimeoutKey, DefaultAzureTryTimeout)
	viper.SetDefault(BlockstoreAzureAuthMethod, DefaultAzureAuthMethod)
}

func reverse(s string) string {
	chars := []rune(s)
	for i := 0; i < len(chars)/2; i++ {
		j := len(chars) - 1 - i
		chars[i], chars[j] = chars[j], chars[i]
	}
	return string(chars)
}

func (c *Config) validateDomainNames() error {
	domainStrings := c.GetS3GatewayDomainNames()
	domainNames := make([]string, len(domainStrings))
	copy(domainNames, domainStrings)
	for i, d := range domainNames {
		domainNames[i] = reverse(d)
	}
	sort.Strings(domainNames)
	for i, d := range domainNames {
		domainNames[i] = reverse(d)
	}
	for i := 0; i < len(domainNames)-1; i++ {
		if strings.HasSuffix(domainNames[i+1], "."+domainNames[i]) {
			return fmt.Errorf("%w: %s, %s", ErrBadDomainNames, domainNames[i], domainNames[i+1])
		}
	}
	return nil
}

func (c *Config) Validate() error {
	missingKeys := ValidateMissingRequiredKeys(c.values, "mapstructure", "squash")
	if len(missingKeys) > 0 {
		return fmt.Errorf("%w: %v", ErrMissingRequiredKeys, missingKeys)
	}

	return nil
}

func (c *Config) GetDatabaseParams() dbparams.Database {
	return dbparams.Database{
		ConnectionString:      c.values.Database.ConnectionString.String(),
		MaxOpenConnections:    c.values.Database.MaxOpenConnections,
		MaxIdleConnections:    c.values.Database.MaxIdleConnections,
		ConnectionMaxLifetime: c.values.Database.ConnectionMaxLifetime,
	}
}

func (c *Config) GetLDAPConfiguration() *LDAP {
	return c.values.Auth.LDAP
}

func (c *Config) GetAwsConfig() (blockparams.AWSParams, error) {
	logger := logging.Default().WithField("sdk", "aws")

	var (
		err            error
		configLoadOpts []func(*aws_config.LoadOptions) error
		yes            = true
		ret            blockparams.AWSParams
	)

	configLoadOpts = append(configLoadOpts,
		func(o *aws_config.LoadOptions) error {
			o.LogConfigurationWarnings = &yes
			return nil
		})

	if c.values.Blockstore.S3.Credentials != nil {
		secretAccessKey := c.values.Blockstore.S3.Credentials.SecretAccessKey
		if secretAccessKey == "" {
			logger.Warn("blockstore.s3.credentials.access_secret_key is deprecated. Use instead: blockstore.s3.credentials.secret_access_key.")
			secretAccessKey = c.values.Blockstore.S3.Credentials.AccessSecretKey
		}
		configLoadOpts = append(configLoadOpts, aws_config.WithCredentialsProvider(
			aws_credentials.NewStaticCredentialsProvider(
				c.values.Blockstore.S3.Credentials.AccessKeyID.String(),
				secretAccessKey.String(),
				c.values.Blockstore.S3.Credentials.SessionToken.String(),
			),
		))
	}

	if c.values.Blockstore.S3.Profile != "" {
		configLoadOpts = append(configLoadOpts, aws_config.WithSharedConfigProfile(c.values.Blockstore.S3.Profile))
	}
	if c.values.Blockstore.S3.CredentialsFile != "" {
		configLoadOpts = append(configLoadOpts, aws_config.WithSharedConfigFiles([]string{c.values.Blockstore.S3.CredentialsFile}))
	}

	ret.EndpointURL = c.values.Blockstore.S3.Endpoint
	ret.Region = c.values.Blockstore.S3.Region

	ret.S3.ForcePathStyle = c.values.Blockstore.S3.ForcePathStyle

	cfg, err := aws_config.LoadDefaultConfig(context.Background(), configLoadOpts...)
	if err != nil {
		return blockparams.AWSParams{}, err
	}
	ret.Config = &cfg

	level := strings.ToLower(logging.Level())
	if level == "trace" {
		ret.Config.ClientLogMode = aws.LogRetries | aws.LogRequest | aws.LogResponse
	}

	ret.Config.Retryer = func() aws.Retryer {
		return aws_retry.NewStandard(
			func(o *aws_retry.StandardOptions) {
				o.MaxAttempts = c.values.Blockstore.S3.MaxRetries
			})
	}
	return ret, nil
}

func (c *Config) GetBlockstoreType() string {
	return c.values.Blockstore.Type
}

func (c *Config) GetBlockAdapterS3Params() (blockparams.S3, error) {
	cfg, err := c.GetAwsConfig()

	if err != nil {
		return blockparams.S3{}, err
	}

	return blockparams.S3{
		AWSParams:             cfg,
		StreamingChunkSize:    c.values.Blockstore.S3.StreamingChunkSize,
		StreamingChunkTimeout: c.values.Blockstore.S3.StreamingChunkTimeout,
		DiscoverBucketRegion:  c.values.Blockstore.S3.DiscoverBucketRegion,
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

func (c *Config) GetS3GatewayDomainNames() []string {
	return c.values.Gateways.S3.DomainNames
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
func (c *Config) GetCommittedTierFSParams(adapter block.Adapter) (*pyramidparams.ExtParams, error) {
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

func (c *Config) GetCommittedBlockStoragePrefix() string {
	return c.values.Committed.BlockStoragePrefix
}

func (c *Config) ToLoggerFields() logging.Fields {
	return MapLoggingFields(c.values)
}

func (c *Config) GetLoggingTraceRequestHeaders() bool {
	return c.values.Logging.TraceRequestHeaders
}
