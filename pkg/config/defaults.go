package config

import (
	"time"

	"github.com/spf13/viper"
)

const (
	DatabaseTypeKey     = "database.type"
	DefaultDatabaseType = "local"

	DatabaseKVLocalPath        = "database.local.path"
	DefaultDatabaseLocalKVPath = "~/lakefs/metadata"

	BlockstoreTypeKey     = "blockstore.type"
	DefaultBlockstoreType = "local"

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

	DefaultListenAddr          = "0.0.0.0:8000"
	DefaultS3GatewayDomainName = "s3.local.lakefs.io"
	DefaultS3GatewayRegion     = "us-east-1"

	DefaultActionsEnabled = true

	DefaultStatsEnabled       = true
	DefaultStatsAddr          = "https://stats.treeverse.io"
	DefaultStatsFlushInterval = time.Second * 30
	DefaultStatsFlushSize     = 100

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

	AuthEncryptSecretKey        = "auth.encrypt.secret_key"            // #nosec
	DefaultAuthEncryptSecretKey = "THIS_MUST_BE_CHANGED_IN_PRODUCTION" // #nosec

	ActionsEnabledKey = "actions.enabled"

	AuthCacheEnabledKey = "auth.cache.enabled"
	AuthCacheSizeKey    = "auth.cache.size"
	AuthCacheTTLKey     = "auth.cache.ttl"
	AuthCacheJitterKey  = "auth.cache.jitter"

	AuthOIDCInitialGroupsClaimName = "auth.oidc.initial_groups_claim_name"
	AuthLogoutRedirectURL          = "auth.logout_redirect_url"

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

	SecurityAuditCheckIntervalKey     = "security.audit_check_interval"
	DefaultSecurityAuditCheckInterval = 24 * time.Hour

	SecurityAuditCheckURLKey     = "security.audit_check_url"
	DefaultSecurityAuditCheckURL = "https://audit.lakefs.io/audit"

	EmailLimitEveryDurationKey = "email.limit_every_duration"
	EmailBurstKey              = "email.burst"
	LakefsEmailBaseURLKey      = "email.lakefs_base_url"

	UIEnabledKey = "ui.enabled"
)

func setDefaultLocalConfig() {
	viper.SetDefault(DatabaseTypeKey, DefaultDatabaseType)
	viper.SetDefault(DatabaseKVLocalPath, DefaultDatabaseLocalKVPath)
	viper.SetDefault(BlockstoreLocalPathKey, DefaultBlockstoreLocalPath)
	viper.SetDefault(AuthEncryptSecretKey, DefaultAuthEncryptSecretKey)
	viper.SetDefault(BlockstoreTypeKey, DefaultBlockstoreType)
}

func setDefaults(local bool) {
	if local {
		setDefaultLocalConfig()
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

	viper.SetDefault(BlockstoreLocalPathKey, DefaultBlockstoreLocalPath)
	viper.SetDefault(BlockstoreTypeKey, DefaultBlockstoreType)
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

	viper.SetDefault(BlockstoreAzureTryTimeoutKey, DefaultBlockstoreAzureTryTimeout)
	viper.SetDefault(BlockstoreAzureAuthMethod, DefaultBlockstoreAzureAuthMethod)

	viper.SetDefault(SecurityAuditCheckIntervalKey, DefaultSecurityAuditCheckInterval)
	viper.SetDefault(SecurityAuditCheckURLKey, DefaultSecurityAuditCheckURL)
	viper.SetDefault(EmailLimitEveryDurationKey, DefaultEmailLimitEveryDuration)
	viper.SetDefault(EmailBurstKey, DefaultEmailBurst)
	viper.SetDefault(LakefsEmailBaseURLKey, DefaultLakefsEmailBaseURL)

	viper.SetDefault(UIEnabledKey, DefaultUIEnabled)
}
