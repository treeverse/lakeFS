package config

import (
	"time"

	"github.com/spf13/viper"
)

const (
	DefaultListenAddress        = "0.0.0.0:8000"
	DefaultLoggingLevel         = "INFO"
	DefaultLoggingAuditLogLevel = "DEBUG"

	BlockstoreTypeKey = "blockstore.type"
)

//nolint:gomnd
func setDefaults(local bool) {
	if local {
		viper.SetDefault("database.type", "local")
		viper.SetDefault("auth.encrypt.secret_key", "THIS_MUST_BE_CHANGED_IN_PRODUCTION") // #nosec
		viper.SetDefault(BlockstoreTypeKey, "local")
	}

	viper.SetDefault("listen_address", DefaultListenAddress)

	viper.SetDefault("logging.format", "text")
	viper.SetDefault("logging.level", DefaultLoggingLevel)
	viper.SetDefault("logging.output", "-")

	viper.SetDefault("logging.files_keep", 100)
	viper.SetDefault("logging.audit_log_level", DefaultLoggingAuditLogLevel)

	viper.SetDefault("logging.file_max_size_mb", (1<<10)*100) // 100MiB

	viper.SetDefault("actions.enabled", true)

	viper.SetDefault("auth.cache.enabled", true)
	viper.SetDefault("auth.cache.size", 1024)
	viper.SetDefault("auth.cache.ttl", 20*time.Second)
	viper.SetDefault("auth.cache.jitter", 3*time.Second)

	viper.SetDefault("auth.oidc.initial_groups_claim_name", "initial_groups")
	viper.SetDefault("auth.logout_redirect_url", "/auth/login")
	viper.SetDefault("auth.login_duration", 7*24*time.Hour)

	viper.SetDefault("blockstore.local.path", "~/lakefs/data/block")
	viper.SetDefault("blockstore.s3.region", "us-east-1")
	viper.SetDefault("blockstore.s3.streaming_chunk_size", 2<<19)          // 1MiB by default per chunk
	viper.SetDefault("blockstore.s3.streaming_chunk_timeout", time.Second) // or 1 seconds, whatever comes first
	viper.SetDefault("blockstore.s3.max_retries", 5)
	viper.SetDefault("blockstore.s3.discover_bucket_region", true)

	viper.SetDefault("committed.local_cache.size_bytes", 1*1024*1024*1024)
	viper.SetDefault("committed.local_cache.dir", "~/lakefs/data/cache")
	viper.SetDefault("committed.local_cache.max_uploaders_per_writer", 10)
	viper.SetDefault("committed.local_cache.range_proportion", 0.9)
	viper.SetDefault("committed.local_cache.metarange_proportion", 0.1)

	viper.SetDefault("committed.block_storage_prefix", "_lakefs")
	viper.SetDefault("committed.permanent.min_range_size_bytes", 0)
	viper.SetDefault("committed.permanent.max_range_size_bytes", 20*1024*1024)
	viper.SetDefault("committed.permanent.range_raggedness_entries", 50_000)
	viper.SetDefault("committed.sstable.memory.cache_size_bytes", 400_000_000)

	viper.SetDefault("gateways.s3.domain_name", "s3.local.lakefs.io")
	viper.SetDefault("gateways.s3.region", "us-east-1")

	viper.SetDefault("blockstore.gs.s3_endpoint", "https://storage.googleapis.com")

	viper.SetDefault("stats.enabled", true)
	viper.SetDefault("stats.address", "https://stats.treeverse.io")
	viper.SetDefault("stats.flush_interval", 30*time.Second)
	viper.SetDefault("stats.flush_size", 100)

	viper.SetDefault("email_subscription.enabled", true)

	viper.SetDefault("blockstore.azure.try_timeout", 10*time.Minute)
	viper.SetDefault("blockstore.azure.auth_method", "access-key")

	viper.SetDefault("security.audit_check_interval", 24*time.Hour)
	viper.SetDefault("security.audit_check_url", "https://audit.lakefs.io/audit")
	viper.SetDefault("email.limit_every_duration", time.Minute)
	viper.SetDefault("email.burst", 10)
	viper.SetDefault("email.lakefs_base_url", "http://localhost:8000")

	viper.SetDefault("ui.enabled", true)

	viper.SetDefault("database.local.path", "~/lakefs/metadata")
	viper.SetDefault("database.local.prefetch_size", 256)
	viper.SetDefault("database.local.sync_writes", true)

	viper.SetDefault("database.dynamodb.table_name", "kvstore")
	viper.SetDefault("database.dynamodb.scan_limit", 1024)

	viper.SetDefault("database.postgres.max_open_connections", 25)
	viper.SetDefault("database.postgres.max_idle_connections", 25)
	viper.SetDefault("database.postgres.connection_max_lifetime", "5m")

	viper.SetDefault("graveler.repository_cache.size", 1000)
	viper.SetDefault("graveler.repository_cache.expiry", 5*time.Second)
	viper.SetDefault("graveler.repository_cache.jitter", 2*time.Second)
	viper.SetDefault("graveler.commit_cache.size", 50_000)
	viper.SetDefault("graveler.commit_cache.expiry", 10*time.Minute)
	viper.SetDefault("graveler.commit_cache.jitter", 2*time.Second)
}
