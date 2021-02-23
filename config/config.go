package config

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/mitchellh/go-homedir"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	authparams "github.com/treeverse/lakefs/auth/params"
	"github.com/treeverse/lakefs/block/factory"
	blockparams "github.com/treeverse/lakefs/block/params"
	dbparams "github.com/treeverse/lakefs/db/params"
	"github.com/treeverse/lakefs/graveler/committed"
	"github.com/treeverse/lakefs/logging"
	pyramidparams "github.com/treeverse/lakefs/pyramid/params"
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

	MetaStoreType          = "metastore.type"
	MetaStoreHiveURI       = "metastore.hive.uri"
	MetastoreGlueCatalogID = "metastore.glue.catalog_id"

	DefaultAzureTryTimeout = 10 * time.Minute
)

var (
	ErrMissingSecretKey  = errors.New("auth.encrypt.secret_key cannot be empty")
	ErrInvalidProportion = errors.New("total proportion isn't 1.0")
)

type LogrusAWSAdapter struct {
	logger *log.Entry
}

func (l *LogrusAWSAdapter) Log(vars ...interface{}) {
	l.logger.Debug(vars...)
}

type Config struct{}

func NewConfig() *Config {
	setDefaults()
	setupLogger()
	return &Config{}
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
	BlockstoreAzureEndpointURLKey               = "blockstore.azure.endpoint"
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
}

type Configurator interface {
	SetDefault(key string, value interface{})
}

type viperConfigurator struct {
}

func (v viperConfigurator) SetDefault(key string, value interface{}) {
	viper.SetDefault(key, value)
}

func (c *Config) Override(fn func(Configurator)) {
	fn(viperConfigurator{})
}

func (c *Config) GetDatabaseParams() dbparams.Database {
	return dbparams.Database{
		ConnectionString:      viper.GetString("database.connection_string"),
		MaxOpenConnections:    viper.GetInt32("database.max_open_connections"),
		MaxIdleConnections:    viper.GetInt32("database.max_idle_connections"),
		ConnectionMaxLifetime: viper.GetDuration("database.connection_max_lifetime"),
	}
}

func (c *Config) GetAwsConfig() *aws.Config {
	cfg := &aws.Config{
		Region: aws.String(viper.GetString(BlockstoreS3RegionKey)),
		Logger: &LogrusAWSAdapter{log.WithField("sdk", "aws")},
	}
	level := strings.ToLower(logging.Level())
	if level == "trace" {
		cfg.LogLevel = aws.LogLevel(aws.LogDebugWithRequestRetries | aws.LogDebugWithRequestErrors)
	}
	if viper.IsSet("blockstore.s3.profile") || viper.IsSet("blockstore.s3.credentials_file") {
		cfg.Credentials = credentials.NewSharedCredentials(
			viper.GetString("blockstore.s3.credentials_file"),
			viper.GetString("blockstore.s3.profile"))
	}
	if viper.IsSet("blockstore.s3.credentials.access_key_id") {
		cfg.Credentials = credentials.NewStaticCredentials(
			viper.GetString("blockstore.s3.credentials.access_key_id"),
			viper.GetString("blockstore.s3.credentials.access_secret_key"),
			viper.GetString("blockstore.s3.credentials.session_token"))
	}

	s3Endpoint := viper.GetString("blockstore.s3.endpoint")
	if len(s3Endpoint) > 0 {
		cfg = cfg.WithEndpoint(s3Endpoint)
	}
	s3ForcePathStyle := viper.GetBool("blockstore.s3.force_path_style")
	if s3ForcePathStyle {
		cfg = cfg.WithS3ForcePathStyle(true)
	}
	cfg = cfg.WithMaxRetries(viper.GetInt(BlockstoreS3MaxRetriesKey))
	return cfg
}

func (c *Config) GetBlockstoreType() string {
	return viper.GetString(BlockstoreTypeKey)
}

func (c *Config) GetBlockAdapterS3Params() (blockparams.S3, error) {
	cfg := c.GetAwsConfig()

	return blockparams.S3{
		AwsConfig:             cfg,
		StreamingChunkSize:    viper.GetInt(BlockstoreS3StreamingChunkSizeKey),
		StreamingChunkTimeout: viper.GetDuration(BlockstoreS3StreamingChunkTimeoutKey),
	}, nil
}

func (c *Config) GetBlockAdapterLocalParams() (blockparams.Local, error) {
	localPath := viper.GetString(BlockstoreLocalPathKey)
	path, err := homedir.Expand(localPath)
	if err != nil {
		return blockparams.Local{}, fmt.Errorf("could not parse blockstore location URI: %w", err)
	}

	return blockparams.Local{Path: path}, err
}

func (c *Config) GetBlockAdapterGSParams() (blockparams.GS, error) {
	return blockparams.GS{
		CredentialsFile: viper.GetString("blockstore.gs.credentials_file"),
		CredentialsJSON: viper.GetString("blockstore.gs.credentials_json"),
	}, nil
}
func (c *Config) GetBlockAdapterAzureParams() (blockparams.Azure, error) {
	return blockparams.Azure{
		StorageAccount:   viper.GetString(BlockstoreAzureStorageAccountKey),
		StorageAccessKey: viper.GetString(BlockstoreAzureStorageAccessKey),
		EndpointURL:      viper.GetString(BlockstoreAzureEndpointURLKey),
		TryTimeout:       viper.GetDuration(BlockstoreAzureTryTimeoutKey),
	}, nil
}

func (c *Config) GetAuthCacheConfig() authparams.ServiceCache {
	return authparams.ServiceCache{
		Enabled:        viper.GetBool(AuthCacheEnabledKey),
		Size:           viper.GetInt(AuthCacheSizeKey),
		TTL:            viper.GetDuration(AuthCacheTTLKey),
		EvictionJitter: viper.GetDuration(AuthCacheJitterKey),
	}
}

func (c *Config) GetAuthEncryptionSecret() []byte {
	secret := viper.GetString("auth.encrypt.secret_key")
	if len(secret) == 0 {
		panic(fmt.Errorf("%w. Please set it to a unique, randomly generated value and store it somewhere safe", ErrMissingSecretKey))
	}
	return []byte(secret)
}

func (c *Config) GetS3GatewayRegion() string {
	return viper.GetString(GatewaysS3RegionKey)
}

func (c *Config) GetS3GatewayDomainName() string {
	return viper.GetString(GatewaysS3DomainNameKey)
}

func (c *Config) GetS3GatewayFallbackURL() string {
	return viper.GetString("gateways.s3.fallback_url")
}

func (c *Config) GetListenAddress() string {
	return viper.GetString(ListenAddressKey)
}

func (c *Config) GetStatsEnabled() bool {
	return viper.GetBool(StatsEnabledKey)
}

func (c *Config) GetStatsAddress() string {
	return viper.GetString(StatsAddressKey)
}

func (c *Config) GetStatsFlushInterval() time.Duration {
	return viper.GetDuration(StatsFlushIntervalKey)
}

const floatSumTolerance = 1e-6

// GetCommittedTierFSParams returns parameters for building a tierFS.  Caller must separately
// build and populate Adapter.
func (c *Config) GetCommittedTierFSParams() (*pyramidparams.ExtParams, error) {
	adapter, err := factory.BuildBlockAdapter(c)
	if err != nil {
		return nil, fmt.Errorf("build block adapter: %w", err)
	}
	rangePro := viper.GetFloat64(CommittedLocalCacheRangeProportionKey)
	metaRangePro := viper.GetFloat64(CommittedLocalCacheMetaRangeProportionKey)

	if math.Abs(rangePro+metaRangePro-1) > floatSumTolerance {
		return nil, fmt.Errorf("range_proportion(%f) and metarange_proportion(%f): %w", rangePro, metaRangePro, ErrInvalidProportion)
	}

	localCacheDir, err := homedir.Expand(viper.GetString(CommittedLocalCacheDirKey))
	if err != nil {
		return nil, fmt.Errorf("expand %s: %w", viper.GetString(CommittedLocalCacheDirKey), err)
	}

	logger := logging.Default().WithField("module", "pyramid")
	return &pyramidparams.ExtParams{
		RangeAllocationProportion:     rangePro,
		MetaRangeAllocationProportion: metaRangePro,
		SharedParams: pyramidparams.SharedParams{
			Logger:             logger,
			Adapter:            adapter,
			BlockStoragePrefix: viper.GetString(CommittedBlockStoragePrefixKey),
			Local: pyramidparams.LocalDiskParams{
				BaseDir:             localCacheDir,
				TotalAllocatedBytes: viper.GetInt64(CommittedLocalCacheSizeBytesKey),
			},
			PebbleSSTableCacheSizeBytes: viper.GetInt64(CommittedPebbleSSTableCacheSizeBytesKey),
		},
	}, nil
}

func (c *Config) GetCommittedParams() *committed.Params {
	return &committed.Params{
		MinRangeSizeBytes:          viper.GetUint64(CommittedPermanentStorageMinRangeSizeKey),
		MaxRangeSizeBytes:          viper.GetUint64(CommittedPermanentStorageMaxRangeSizeKey),
		RangeSizeEntriesRaggedness: viper.GetFloat64(CommittedPermanentStorageRangeRaggednessKey),
		MaxUploaders:               viper.GetInt(CommittedLocalCacheNumUploadersKey),
	}
}

func GetMetastoreAwsConfig() *aws.Config {
	cfg := &aws.Config{
		Region: aws.String(viper.GetString("metastore.glue.region")),
		Logger: &LogrusAWSAdapter{},
	}
	if viper.IsSet("metastore.glue.profile") || viper.IsSet("metastore.glue.credentials_file") {
		cfg.Credentials = credentials.NewSharedCredentials(
			viper.GetString("metastore.glue.credentials_file"),
			viper.GetString("metastore.glue.profile"))
	}
	if viper.IsSet("metastore.glue.credentials.access_key_id") {
		cfg.Credentials = credentials.NewStaticCredentials(
			viper.GetString("metastore.glue.credentials.access_key_id"),
			viper.GetString("metastore.glue.credentials.access_secret_key"),
			viper.GetString("metastore.glue.credentials.session_token"))
	}
	return cfg
}

func GetMetastoreHiveURI() string {
	return viper.GetString(MetaStoreHiveURI)
}

func GetMetastoreGlueCatalogID() string {
	return viper.GetString(MetastoreGlueCatalogID)
}
func GetMetastoreType() string {
	return viper.GetString(MetaStoreType)
}

func GetFixedInstallationID() string {
	return viper.GetString("installation.fixed_id")
}
