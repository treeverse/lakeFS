package config

import (
	"time"

	"github.com/spf13/viper"
)

const (
	DatabaseTypeKey   = "database.type"
	LocalDatabaseType = "local"

	DatabaseLocalPathKey     = "database.local.path"
	DefaultDatabaseLocalPath = "~/lakefs/metadata"

	DatabaseLocalPrefetchSizeKey     = "database.local.prefetch_size"
	DefaultDatabaseLocalPrefetchSize = 256

	DatabaseLocalSyncWritesKey     = "database.local.sync_writes"
	DefaultDatabaseLocalSyncWrites = true

	DatabaseDynamodbTableNameKey     = "database.dynamodb.table_name"
	DefaultDatabaseDynamodbTableName = "kvstore"

	DatabaseDynamodbReadCapacityUnitsKey     = "database.dynamodb.read_capacity_units"
	DefaultDatabaseDynamodbReadCapacityUnits = 1000

	DatabaseDynamodbWriteCapacityUnitsKey     = "database.dynamodb.write_capacity_units"
	DefaultDatabaseDynamodbWriteCapacityUnits = 1000

	DatabasePostgresMaxOpenConnectionsKey     = "database.postgres.max_open_connections"
	DefaultDatabasePostgresMaxOpenConnections = 25

	DatabasePostgresMaxIdleConnectionsKey     = "database.postgres.max_idle_connections"
	DefaultDatabasePostgresMaxIdleConnections = 25

	PostgresConnectionMaxLifetimeKey     = "database.postgres.connection_max_lifetime"
	DefaultPostgresConnectionMaxLifetime = "5m"

	BlockstoreTypeKey   = "blockstore.type"
	LocalBlockstoreType = "local"

	BlockstoreLocalPathKey     = "blockstore.local.path"
	DefaultBlockstoreLocalPath = "~/lakefs/data/block"

	BlockstoreS3RegionKey     = "blockstore.s3.region"
	DefaultBlockstoreS3Region = "us-east-1"

	BlockstoreS3StreamingChunkSizeKey     = "blockstore.s3.streaming_chunk_size"
	DefaultBlockstoreS3StreamingChunkSize = 2 << 19 // 1MiB by default per chunk

	BlockstoreS3StreamingChunkTimeoutKey     = "blockstore.s3.streaming_chunk_timeout"
	DefaultBlockstoreS3StreamingChunkTimeout = time.Second * 1 // or 1 seconds, whatever comes first

	BlockstoreS3DiscoverBucketRegionKey     = "blockstore.s3.discover_bucket_region"
	DefaultBlockstoreS3DiscoverBucketRegion = true

	BlockstoreS3MaxRetriesKey     = "blockstore.s3.max_retries"
	DefaultBlockstoreS3MaxRetries = 5

	BlockstoreAzureTryTimeoutKey     = "blockstore.azure.try_timeout"
	DefaultBlockstoreAzureTryTimeout = 10 * time.Minute

	BlockstoreAzureAuthMethod        = "blockstore.azure.auth_method"
	DefaultBlockstoreAzureAuthMethod = "access-key"

	BlockstoreGSS3EndpointKey     = "blockstore.gs.s3_endpoint"
	DefaultBlockstoreGSS3Endpoint = "https://storage.googleapis.com"

	DefaultCommittedLocalCacheRangePercent          = 0.9
	DefaultCommittedLocalCacheMetaRangePercent      = 0.1
	DefaultCommittedLocalCacheBytes                 = 1 * 1024 * 1024 * 1024
	DefaultCommittedLocalCacheDir                   = "~/lakefs/data/cache"
	DefaultCommittedPebbleSSTableCacheSizeBytes     = 400_000_000
	DefaultCommittedLocalCacheNumUploaders          = 10
	DefaultCommittedBlockStoragePrefix              = "_lakefs"
	DefaultCommittedPermanentMinRangeSizeBytes      = 0
	DefaultCommittedPermanentMaxRangeSizeBytes      = 20 * 1024 * 1024
	DefaultCommittedPermanentRangeRaggednessEntries = 50_000

	DefaultAuthCacheEnabled = true
	DefaultAuthCacheSize    = 1024
	DefaultAuthCacheTTL     = 20 * time.Second
	DefaultAuthCacheJitter  = 3 * time.Second

	DefaultAuthOIDCInitialGroupsClaimName = "initial_groups"
	DefaultAuthLogoutRedirectURL          = "/auth/login"
	DefaultAuthLoginDuration              = 7 * 24 * time.Hour

	DefaultListenAddr          = "0.0.0.0:8000"
	DefaultS3GatewayDomainName = "s3.local.lakefs.io"
	DefaultS3GatewayRegion     = "us-east-1"

	DefaultActionsEnabled = true

	DefaultStatsEnabled       = true
	DefaultStatsAddr          = "https://stats.treeverse.io"
	DefaultStatsFlushInterval = time.Second * 30
	DefaultStatsFlushSize     = 100

	DefaultEmailSubscriptionEnabled = true

	DefaultEmailLimitEveryDuration = time.Minute
	DefaultEmailBurst              = 10
	DefaultLakefsEmailBaseURL      = "http://localhost:8000"

	DefaultUIEnabled = true

	ListenAddressKey = "listen_address"

	LoggingFormatKey        = "logging.format"
	LoggingLevelKey         = "logging.level"
	LoggingOutputKey        = "logging.output"
	LoggingFileMaxSizeMBKey = "logging.file_max_size_mb"
	LoggingFilesKeepKey     = "logging.files_keep"
	LoggingAuditLogLevel    = "logging.audit_log_level"

	AuthEncryptSecretKey      = "auth.encrypt.secret_key"            // #nosec
	LocalAuthEncryptSecretKey = "THIS_MUST_BE_CHANGED_IN_PRODUCTION" // #nosec

	ActionsEnabledKey = "actions.enabled"

	AuthCacheEnabledKey = "auth.cache.enabled"
	AuthCacheSizeKey    = "auth.cache.size"
	AuthCacheTTLKey     = "auth.cache.ttl"
	AuthCacheJitterKey  = "auth.cache.jitter"

	AuthOIDCInitialGroupsClaimName = "auth.oidc.initial_groups_claim_name"
	AuthLogoutRedirectURL          = "auth.logout_redirect_url"
	AuthLoginDuration              = "auth.login_duration"

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

	StatsEnabledKey       = "stats.enabled"
	StatsAddressKey       = "stats.address"
	StatsFlushIntervalKey = "stats.flush_interval"
	StatsFlushSizeKey     = "stats.flush_size"

	EmailSubscriptionEnabledKey = "email_subscription.enabled"

	SecurityAuditCheckIntervalKey     = "security.audit_check_interval"
	DefaultSecurityAuditCheckInterval = 24 * time.Hour

	SecurityAuditCheckURLKey     = "security.audit_check_url"
	DefaultSecurityAuditCheckURL = "https://audit.lakefs.io/audit"

	EmailLimitEveryDurationKey = "email.limit_every_duration"
	EmailBurstKey              = "email.burst"
	LakefsEmailBaseURLKey      = "email.lakefs_base_url"

	UIEnabledKey = "ui.enabled"

	GravelerRepositoryCacheSizeKey       = "graveler.repository_cache.size"
	DefaultGravelerRepositoryCacheSize   = 1000
	GravelerRepositoryCacheExpiryKey     = "graveler.repository_cache.expiry"
	DefaultGravelerRepositoryCacheExpiry = 5 * time.Second
	GravelerRepositoryCacheJitterKey     = "graveler.repository_cache.jitter"
	DefaultGravelerRepositoryCacheJitter = 2 * time.Second
	GravelerCommitCacheSizeKey           = "graveler.commit_cache.size"
	DefaultGravelerCommitCacheSize       = 60_000
	GravelerCommitCacheExpiryKey         = "graveler.commit_cache.expiry"
	DefaultGravelerCommitCacheExpiry     = 10 * time.Minute
	GravelerCommitCacheJitterKey         = "graveler.commit_cache.jitter"
	DefaultGravelerCommitCacheJitter     = 2 * time.Second
	GravelerBranchCacheSizeKey           = "graveler.branch_cache.size"
	DefaultGravelerBranchCacheSize       = 500
	GravelerBranchCacheExpiryKey         = "graveler.commit_cache.expiry"
	DefaultGravelerBranchCacheExpiry     = 10 * time.Minute
	GravelerBranchCacheJitterKey         = "graveler.commit_cache.jitter"
	DefaultGravelerBranchCacheJitter     = 2 * time.Second
)

func setDefaults(local bool) {
	if local {
		viper.SetDefault(DatabaseTypeKey, LocalDatabaseType)
		viper.SetDefault(AuthEncryptSecretKey, LocalAuthEncryptSecretKey)
		viper.SetDefault(BlockstoreTypeKey, LocalBlockstoreType)
	}

	viper.SetDefault(ListenAddressKey, DefaultListenAddr)

	viper.SetDefault(LoggingFormatKey, DefaultLoggingFormat)
	viper.SetDefault(LoggingLevelKey, DefaultLoggingLevel)
	viper.SetDefault(LoggingOutputKey, DefaultLoggingOutput)
	viper.SetDefault(LoggingFilesKeepKey, DefaultLoggingFilesKeepKey)
	viper.SetDefault(LoggingAuditLogLevel, DefaultAuditLogLevel)

	viper.SetDefault(ActionsEnabledKey, DefaultActionsEnabled)

	viper.SetDefault(AuthCacheEnabledKey, DefaultAuthCacheEnabled)
	viper.SetDefault(AuthCacheSizeKey, DefaultAuthCacheSize)
	viper.SetDefault(AuthCacheTTLKey, DefaultAuthCacheTTL)
	viper.SetDefault(AuthCacheJitterKey, DefaultAuthCacheJitter)

	viper.SetDefault(AuthOIDCInitialGroupsClaimName, DefaultAuthOIDCInitialGroupsClaimName)
	viper.SetDefault(AuthLogoutRedirectURL, DefaultAuthLogoutRedirectURL)
	viper.SetDefault(AuthLoginDuration, DefaultAuthLoginDuration)

	viper.SetDefault(BlockstoreLocalPathKey, DefaultBlockstoreLocalPath)
	viper.SetDefault(BlockstoreS3RegionKey, DefaultBlockstoreS3Region)
	viper.SetDefault(BlockstoreS3StreamingChunkSizeKey, DefaultBlockstoreS3StreamingChunkSize)
	viper.SetDefault(BlockstoreS3StreamingChunkTimeoutKey, DefaultBlockstoreS3StreamingChunkTimeout)
	viper.SetDefault(BlockstoreS3MaxRetriesKey, DefaultBlockstoreS3MaxRetries)
	viper.SetDefault(BlockstoreS3StreamingChunkSizeKey, DefaultBlockstoreS3StreamingChunkSize)
	viper.SetDefault(BlockstoreS3DiscoverBucketRegionKey, DefaultBlockstoreS3DiscoverBucketRegion)

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

	viper.SetDefault(BlockstoreGSS3EndpointKey, DefaultBlockstoreGSS3Endpoint)

	viper.SetDefault(StatsEnabledKey, DefaultStatsEnabled)
	viper.SetDefault(StatsAddressKey, DefaultStatsAddr)
	viper.SetDefault(StatsFlushIntervalKey, DefaultStatsFlushInterval)
	viper.SetDefault(StatsFlushSizeKey, DefaultStatsFlushSize)

	viper.SetDefault(EmailSubscriptionEnabledKey, DefaultEmailSubscriptionEnabled)

	viper.SetDefault(BlockstoreAzureTryTimeoutKey, DefaultBlockstoreAzureTryTimeout)
	viper.SetDefault(BlockstoreAzureAuthMethod, DefaultBlockstoreAzureAuthMethod)

	viper.SetDefault(SecurityAuditCheckIntervalKey, DefaultSecurityAuditCheckInterval)
	viper.SetDefault(SecurityAuditCheckURLKey, DefaultSecurityAuditCheckURL)
	viper.SetDefault(EmailLimitEveryDurationKey, DefaultEmailLimitEveryDuration)
	viper.SetDefault(EmailBurstKey, DefaultEmailBurst)
	viper.SetDefault(LakefsEmailBaseURLKey, DefaultLakefsEmailBaseURL)

	viper.SetDefault(UIEnabledKey, DefaultUIEnabled)

	viper.SetDefault(BlockstoreLocalPathKey, DefaultBlockstoreLocalPath)

	viper.SetDefault(DatabaseLocalPathKey, DefaultDatabaseLocalPath)
	viper.SetDefault(DatabaseLocalPrefetchSizeKey, DefaultDatabaseLocalPrefetchSize)
	viper.SetDefault(DatabaseLocalSyncWritesKey, DefaultDatabaseLocalSyncWrites)

	viper.SetDefault(DatabaseDynamodbTableNameKey, DefaultDatabaseDynamodbTableName)

	viper.SetDefault(DatabaseDynamodbReadCapacityUnitsKey, DefaultDatabaseDynamodbReadCapacityUnits)
	viper.SetDefault(DatabaseDynamodbWriteCapacityUnitsKey, DefaultDatabaseDynamodbWriteCapacityUnits)

	viper.SetDefault(DatabasePostgresMaxOpenConnectionsKey, DefaultDatabasePostgresMaxOpenConnections)
	viper.SetDefault(DatabasePostgresMaxIdleConnectionsKey, DefaultDatabasePostgresMaxIdleConnections)
	viper.SetDefault(PostgresConnectionMaxLifetimeKey, DefaultPostgresConnectionMaxLifetime)

	viper.SetDefault(GravelerRepositoryCacheSizeKey, DefaultGravelerRepositoryCacheSize)
	viper.SetDefault(GravelerRepositoryCacheExpiryKey, DefaultGravelerRepositoryCacheExpiry)
	viper.SetDefault(GravelerCommitCacheSizeKey, DefaultGravelerCommitCacheSize)
	viper.SetDefault(GravelerCommitCacheExpiryKey, DefaultGravelerCommitCacheExpiry)
}
