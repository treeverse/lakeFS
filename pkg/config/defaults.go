package config

import (
	"time"

	"github.com/spf13/viper"
)

const (
	DefaultListenAddress        = "0.0.0.0:8000"
	DefaultLoggingLevel         = "INFO"
	DefaultLoggingAuditLogLevel = "DEBUG"
	BlockstoreTypeKey           = "blockstore.type"
	DefaultQuickstartUsername   = "quickstart"
	// quicksart creds, safe
	DefaultQuickstartKeyID           = "AKIAIOSFOLQUICKSTART"                     //nolint:gosec
	DefaultQuickstartSecretKey       = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" // nolint:gosec
	DefaultAuthAPIHealthCheckTimeout = 20 * time.Second
	DefaultAuthSecret                = "THIS_MUST_BE_CHANGED_IN_PRODUCTION"   // #nosec
	DefaultSigningSecretKey          = "OVERRIDE_THIS_SIGNING_SECRET_DEFAULT" // #nosec
	// storage blockstore values
	DefaultBlockstoreLocalPath                        = "~/lakefs/data/block" // #nosec
	DefaultBlockstoreS3Region                         = "us-east-1"           // #nosec
	DefaultBlockstoreS3MaxRetries                     = 5
	DefaultBlockstoreS3DiscoverBucketRegion           = true
	DefaultBlockstoreS3PreSignedExpiry                = 15 * time.Minute
	DefaultBlockstoreS3WebIdentitySessionExpiryWindow = 5 * time.Minute
	DefaultBlockstoreS3DisablePreSignedUI             = true
	DefaultBlockstoreGSS3Endpoint                     = "https://storage.googleapis.com" // #nosec
	DefaultBlockstoreGSPreSignedExpiry                = 15 * time.Minute
	DefaultBlockstoreGSDisablePreSignedUI             = true
	DefaultBlockstoreAzureTryTimeout                  = 10 * time.Minute
	DefaultBlockstoreAzurePreSignedExpiry             = 15 * time.Minute
	DefaultBlockstoreAzureDisablePreSignedUI          = true
)

//nolint:mnd
func setBaseDefaults(cfgType string) {
	switch cfgType {
	case QuickstartConfiguration:
		viper.SetDefault("installation.user_name", DefaultQuickstartUsername)
		viper.SetDefault("installation.access_key_id", DefaultQuickstartKeyID)
		viper.SetDefault("installation.secret_access_key", DefaultQuickstartSecretKey)
		viper.SetDefault("database.type", "local")
		viper.SetDefault("auth.encrypt.secret_key", DefaultAuthSecret)
		viper.SetDefault(BlockstoreTypeKey, "local")
	case UseLocalConfiguration:
		viper.SetDefault("database.type", "local")
		viper.SetDefault("auth.encrypt.secret_key", DefaultAuthSecret)
		viper.SetDefault(BlockstoreTypeKey, "local")
	}

	viper.SetDefault("installation.allow_inter_region_storage", true)

	viper.SetDefault("listen_address", DefaultListenAddress)

	viper.SetDefault("logging.format", "text")
	viper.SetDefault("logging.level", DefaultLoggingLevel)
	viper.SetDefault("logging.output", "-")
	viper.SetDefault("logging.files_keep", 100)
	viper.SetDefault("logging.audit_log_level", DefaultLoggingAuditLogLevel)
	viper.SetDefault("logging.file_max_size_mb", (1<<10)*100) // 100MiB

	viper.SetDefault("actions.enabled", true)
	viper.SetDefault("actions.env.enabled", true)
	viper.SetDefault("actions.env.prefix", "LAKEFSACTION_")

	viper.SetDefault("auth.cache.enabled", true)
	viper.SetDefault("auth.cache.size", 1024)
	viper.SetDefault("auth.cache.ttl", 20*time.Second)
	viper.SetDefault("auth.cache.jitter", 3*time.Second)

	viper.SetDefault("auth.logout_redirect_url", "/auth/login")

	viper.SetDefault("auth.login_duration", 7*24*time.Hour)
	viper.SetDefault("auth.login_max_duration", 14*24*time.Hour)

	viper.SetDefault("auth.ui_config.rbac", "none")
	viper.SetDefault("auth.ui_config.login_failed_message", "The credentials don't match.")
	viper.SetDefault("auth.ui_config.login_cookie_names", "internal_auth_session")

	viper.SetDefault("auth.remote_authenticator.default_user_group", "Viewers")
	viper.SetDefault("auth.remote_authenticator.request_timeout", 10*time.Second)

	viper.SetDefault("auth.api.health_check_timeout", DefaultAuthAPIHealthCheckTimeout)
	viper.SetDefault("auth.oidc.persist_friendly_name", false)
	viper.SetDefault("auth.cookie_auth_verification.persist_friendly_name", false)

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
	viper.SetDefault("gateways.s3.verify_unsupported", true)

	// blockstore defaults
	viper.SetDefault("blockstore.signing.secret_key", DefaultSigningSecretKey)

	viper.SetDefault("blockstore.local.path", DefaultBlockstoreLocalPath)

	viper.SetDefault("blockstore.s3.region", DefaultBlockstoreS3Region)
	viper.SetDefault("blockstore.s3.max_retries", DefaultBlockstoreS3MaxRetries)
	viper.SetDefault("blockstore.s3.discover_bucket_region", DefaultBlockstoreS3DiscoverBucketRegion)
	viper.SetDefault("blockstore.s3.pre_signed_expiry", DefaultBlockstoreS3PreSignedExpiry)
	viper.SetDefault("blockstore.s3.web_identity.session_expiry_window", DefaultBlockstoreS3WebIdentitySessionExpiryWindow)
	viper.SetDefault("blockstore.s3.disable_pre_signed_ui", DefaultBlockstoreS3DisablePreSignedUI)

	viper.SetDefault("blockstore.gs.s3_endpoint", DefaultBlockstoreGSS3Endpoint)
	viper.SetDefault("blockstore.gs.pre_signed_expiry", DefaultBlockstoreGSPreSignedExpiry)
	viper.SetDefault("blockstore.gs.disable_pre_signed_ui", DefaultBlockstoreGSDisablePreSignedUI)

	viper.SetDefault("blockstore.azure.try_timeout", DefaultBlockstoreAzureTryTimeout)
	viper.SetDefault("blockstore.azure.pre_signed_expiry", DefaultBlockstoreAzurePreSignedExpiry)
	viper.SetDefault("blockstore.azure.disable_pre_signed_ui", DefaultBlockstoreAzureDisablePreSignedUI)

	viper.SetDefault("stats.enabled", true)
	viper.SetDefault("stats.address", "https://stats.lakefs.io")
	viper.SetDefault("stats.flush_interval", 30*time.Second)
	viper.SetDefault("stats.flush_size", 100)

	viper.SetDefault("email_subscription.enabled", true)

	viper.SetDefault("security.audit_check_interval", 24*time.Hour)
	viper.SetDefault("security.audit_check_url", "https://audit.lakefs.io/audit")
	viper.SetDefault("security.check_latest_version", true)
	viper.SetDefault("security.check_latest_version_cache", time.Hour)

	viper.SetDefault("ui.enabled", true)

	viper.SetDefault("database.local.path", "~/lakefs/metadata")
	viper.SetDefault("database.local.prefetch_size", 256)
	viper.SetDefault("database.local.sync_writes", true)

	viper.SetDefault("database.dynamodb.table_name", "kvstore")
	viper.SetDefault("database.dynamodb.scan_limit", 1024)
	viper.SetDefault("database.dynamodb.max_attempts", 10)

	viper.SetDefault("database.postgres.max_open_connections", 25)
	viper.SetDefault("database.postgres.max_idle_connections", 25)
	viper.SetDefault("database.postgres.connection_max_lifetime", "5m")

	viper.SetDefault("graveler.ensure_readable_root_namespace", true)
	viper.SetDefault("graveler.repository_cache.size", 1000)
	viper.SetDefault("graveler.repository_cache.expiry", 5*time.Second)
	viper.SetDefault("graveler.repository_cache.jitter", 2*time.Second)
	viper.SetDefault("graveler.commit_cache.size", 50_000)
	viper.SetDefault("graveler.commit_cache.expiry", 10*time.Minute)
	viper.SetDefault("graveler.commit_cache.jitter", 2*time.Second)

	// MaxBatchDelay - 3ms was chosen as a max delay time for critical path queries.
	// It trades off amount of queries per second (and thus effectiveness of the batching mechanism) with added latency.
	// Since reducing # of expensive operations is only beneficial when there are a lot of concurrent requests,
	//
	//	the sweet spot is probably between 1-5 milliseconds (representing 200-1000 requests/second to the data store).
	//
	// 3ms of delay with ~300 requests/second per resource sounds like a reasonable tradeoff.
	viper.SetDefault("graveler.max_batch_delay", 3*time.Millisecond)

	viper.SetDefault("graveler.branch_ownership.enabled", false)
	// ... but if branch ownership is enabled, set up some useful defaults!

	// The single concurrent branch updater has these requirements from
	// KV with these settings:
	//
	//   - Cleanly acquiring ownership performs 1 read operation and 1
	//     write operation.  Releasing ownership performs another 1 read
	//     operation and 1 write operation.
	//
	//   - While ownership is held, add 2.5 read and 2.5 write operation
	//     per second, an additional ~7 read operations per second per
	//     branch operation waiting to acquire ownership, and an
	//     additional write operation per branch operation acquiring
	//     ownership.

	// See additional comments on MostlyCorrectOwner for how to compute these numbers.
	viper.SetDefault("graveler.branch_ownership.refresh", 400*time.Millisecond)
	viper.SetDefault("graveler.branch_ownership.acquire", 150*time.Millisecond)

	viper.SetDefault("ugc.prepare_interval", time.Minute)
	viper.SetDefault("ugc.prepare_max_file_size", 20*1024*1024)

	viper.SetDefault("usage_report.enabled", true)
	viper.SetDefault("usage_report.flush_interval", 5*time.Minute)
}
