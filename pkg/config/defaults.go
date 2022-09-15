package config

import (
	"time"

	"github.com/spf13/viper"
)

const (
	BlockStoreTypeKey     = "blockstore.type"
	DefaultBlockStoreType = "local"

	BlockStoreLocalPathKey     = "blockstore.local.path"
	DefaultBlockStoreLocalPath = "~/lakefs/data/block"

	BlockStoreS3RegionKey     = "blockstore.s3.region"
	DefaultBlockStoreS3Region = "us-east-1"

	BlockStoreS3StreamingChunkSizeKey     = "blockstore.s3.streaming_chunk_size"
	DefaultBlockStoreS3StreamingChunkSize = 2 << 19 // 1MiB by default per chunk

	BlockStoreS3StreamingChunkTimeoutKey     = "blockstore.s3.streaming_chunk_timeout"
	DefaultBlockStoreS3StreamingChunkTimeout = time.Second * 1 // or 1 seconds, whatever comes first

	BlockStoreS3DiscoverBucketRegionKey     = "blockstore.s3.discover_bucket_region"
	DefaultBlockStoreS3DiscoverBucketRegion = true

	BlockStoreS3MaxRetriesKey     = "blockstore.s3.max_retries"
	DefaultBlockStoreS3MaxRetries = 5

	BlockStoreAzureTryTimeoutKey     = "blockstore.azure.try_timeout"
	DefaultBlockStoreAzureTryTimeout = 10 * time.Minute

	BlockStoreAzureAuthMethod        = "blockstore.azure.auth_method"
	DefaultBlockStoreAzureAuthMethod = "access-key"

	BlockStoreGSS3EndpointKey     = "blockstore.gs.s3_endpoint"
	DefaultBlockStoreGSS3Endpoint = "https://storage.googleapis.com"

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

	SecurityAuditCheckIntervalKey     = "security.audit_check_interval"
	DefaultSecurityAuditCheckInterval = 12 * time.Hour

	SecurityAuditCheckURLKey     = "security.audit_check_url"
	DefaultSecurityAuditCheckURL = "https://audit.lakefs.io/audit"

	EmailLimitEveryDurationKey = "email.limit_every_duration"
	EmailBurstKey              = "email.burst"
	LakefsEmailBaseURLKey      = "email.lakefs_base_url"

	UIEnabledKey = "ui.enabled"
)

func setDefaults() {
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

	viper.SetDefault(BlockStoreLocalPathKey, DefaultBlockStoreLocalPath)
	viper.SetDefault(BlockStoreTypeKey, DefaultBlockStoreType)
	viper.SetDefault(BlockStoreS3RegionKey, DefaultBlockStoreS3Region)
	viper.SetDefault(BlockStoreS3StreamingChunkSizeKey, DefaultBlockStoreS3StreamingChunkSize)
	viper.SetDefault(BlockStoreS3StreamingChunkTimeoutKey, DefaultBlockStoreS3StreamingChunkTimeout)
	viper.SetDefault(BlockStoreS3MaxRetriesKey, DefaultBlockStoreS3MaxRetries)
	viper.SetDefault(BlockStoreS3StreamingChunkSizeKey, DefaultBlockStoreS3StreamingChunkSize)
	viper.SetDefault(BlockStoreS3DiscoverBucketRegionKey, DefaultBlockStoreS3DiscoverBucketRegion)

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

	viper.SetDefault(BlockStoreGSS3EndpointKey, DefaultBlockStoreGSS3Endpoint)

	viper.SetDefault(StatsEnabledKey, DefaultStatsEnabled)
	viper.SetDefault(StatsAddressKey, DefaultStatsAddr)
	viper.SetDefault(StatsFlushIntervalKey, DefaultStatsFlushInterval)

	viper.SetDefault(BlockStoreAzureTryTimeoutKey, DefaultBlockStoreAzureTryTimeout)
	viper.SetDefault(BlockStoreAzureAuthMethod, DefaultBlockStoreAzureAuthMethod)

	viper.SetDefault(SecurityAuditCheckIntervalKey, DefaultSecurityAuditCheckInterval)
	viper.SetDefault(SecurityAuditCheckURLKey, DefaultSecurityAuditCheckURL)
	viper.SetDefault(EmailLimitEveryDurationKey, DefaultEmailLimitEveryDuration)
	viper.SetDefault(EmailBurstKey, DefaultEmailBurst)
	viper.SetDefault(LakefsEmailBaseURLKey, DefaultLakefsEmailBaseURL)

	viper.SetDefault(UIEnabledKey, DefaultUIEnabled)
}
