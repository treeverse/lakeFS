package config

import (
	"time"
)

// LDAP holds configuration for authenticating on an LDAP server.
type LDAP struct {
	ServerEndpoint    string `mapstructure:"server_endpoint"`
	BindDN            string `mapstructure:"bind_dn"`
	BindPassword      string `mapstructure:"bind_password"`
	DefaultUserGroup  string `mapstructure:"default_user_group"`
	UsernameAttribute string `mapstructure:"username_attribute"`
	UserBaseDN        string `mapstructure:"user_base_dn"`
	UserFilter        string `mapstructure:"user_filter"`
}

// S3AuthInfo holds S3-style authentication.
type S3AuthInfo struct {
	CredentialsFile string `mapstructure:"credentials_file"`
	Profile         string
	Credentials     *struct {
		AccessKeyID SecureString `mapstructure:"access_key_id"`
		// AccessSecretKey is the old name for SecretAccessKey.
		//
		// Deprecated: use SecretAccessKey instead.
		AccessSecretKey SecureString `mapstructure:"access_secret_key"`
		SecretAccessKey SecureString `mapstructure:"secret_access_key"`
		SessionToken    SecureString `mapstructure:"session_token"`
	}
}

// Output struct of configuration, used to validate.  If you read a key using a viper accessor
// rather than accessing a field of this struct, that key will *not* be validated.  So don't
// do that.
type configuration struct {
	ListenAddress string `mapstructure:"listen_address"`

	Actions struct {
		// ActionsEnabled set to false will block any hook execution
		Enabled bool `mapstructure:"enabled"`
	}

	Logging struct {
		Format        string   `mapstructure:"format"`
		Level         string   `mapstructure:"level"`
		Output        []string `mapstructure:"output"`
		FileMaxSizeMB int      `mapstructure:"file_max_size_mb"`
		FilesKeep     int      `mapstructure:"files_keep"`
		AuditLogLevel string   `mapstructure:"audit_log_level"`
		// TraceRequestHeaders work only on 'trace' level, default is false as it may log sensitive data to the log
		TraceRequestHeaders bool `mapstructure:"trace_request_headers"`
	}

	Database struct {
		ConnectionString      SecureString  `mapstructure:"connection_string"`
		MaxOpenConnections    int32         `mapstructure:"max_open_connections"`
		MaxIdleConnections    int32         `mapstructure:"max_idle_connections"`
		ConnectionMaxLifetime time.Duration `mapstructure:"connection_max_lifetime"`
		// KVEnabled Development flag to switch between postgres DB and KV store implementations
		KVEnabled bool `mapstructure:"kv_enabled"`
		// DropTables Development flag to delete tables after successful migration to KV
		DropTables bool `mapstructure:"drop_tables"`
		// Type  Name of the KV Store driver DB implementation which is available according to the kv package Drivers function
		Type string `mapstructure:"type"`
	}

	Auth struct {
		Cache struct {
			Enabled bool
			Size    int
			TTL     time.Duration
			Jitter  time.Duration
		}
		Encrypt struct {
			SecretKey SecureString `mapstructure:"secret_key" validate:"required"`
		}
		API struct {
			Endpoint string
			Token    string
		}
		LDAP         *LDAP
		CookieDomain string `mapstructure:"cookie_domain"`
	}
	Blockstore struct {
		Type                   string `validate:"required"`
		DefaultNamespacePrefix string `mapstructure:"default_namespace_prefix"`
		Local                  *struct {
			Path string
		}
		S3 *struct {
			S3AuthInfo            `mapstructure:",squash"`
			Region                string
			Endpoint              string
			StreamingChunkSize    int           `mapstructure:"streaming_chunk_size"`
			StreamingChunkTimeout time.Duration `mapstructure:"streaming_chunk_timeout"`
			MaxRetries            int           `mapstructure:"max_retries"`
			ForcePathStyle        bool          `mapstructure:"force_path_style"`
			DiscoverBucketRegion  bool          `mapstructure:"discover_bucket_region"`
		}
		Azure *struct {
			TryTimeout       time.Duration `mapstructure:"try_timeout"`
			StorageAccount   string        `mapstructure:"storage_account"`
			StorageAccessKey string        `mapstructure:"storage_access_key"`
			AuthMethod       string        `mapstructure:"auth_method"`
		}
		GS *struct {
			S3Endpoint      string `mapstructure:"s3_endpoint"`
			CredentialsFile string `mapstructure:"credentials_file"`
			CredentialsJSON string `mapstructure:"credentials_json"`
		}
	}
	Committed struct {
		LocalCache struct {
			SizeBytes             int64 `mapstructure:"size_bytes"`
			Dir                   string
			MaxUploadersPerWriter int     `mapstructure:"max_uploaders_per_writer"`
			RangeProportion       float64 `mapstructure:"range_proportion"`
			MetaRangeProportion   float64 `mapstructure:"metarange_proportion"`
		} `mapstructure:"local_cache"`
		BlockStoragePrefix string `mapstructure:"block_storage_prefix"`
		Permanent          struct {
			MinRangeSizeBytes      uint64  `mapstructure:"min_range_size_bytes"`
			MaxRangeSizeBytes      uint64  `mapstructure:"max_range_size_bytes"`
			RangeRaggednessEntries float64 `mapstructure:"range_raggedness_entries"`
		}
		SSTable struct {
			Memory struct {
				CacheSizeBytes int64 `mapstructure:"cache_size_bytes"`
			}
		}
	}
	Gateways struct {
		S3 struct {
			DomainNames Strings `mapstructure:"domain_name"`
			Region      string
			FallbackURL string `mapstructure:"fallback_url"`
		}
	}
	Stats struct {
		Enabled       bool
		Address       string
		FlushInterval time.Duration `mapstructure:"flush_interval"`
	}
	Installation struct {
		FixedID string `mapstructure:"fixed_id"`
	}
	Security struct {
		AuditCheckInterval time.Duration `mapstructure:"audit_check_interval"`
		AuditCheckURL      string        `mapstructure:"audit_check_url"`
	} `mapstructure:"security"`
	Email struct {
		SMTPHost           string        `mapstructure:"smtp_host"`
		SMTPPort           int           `mapstructure:"smtp_port"`
		UseSSL             bool          `mapstructure:"use_ssl"`
		Username           string        `mapstructure:"username"`
		Password           string        `mapstructure:"password"`
		LocalName          string        `mapstructure:"local_name"`
		Sender             string        `mapstructure:"sender"`
		LimitEveryDuration time.Duration `mapstructure:"limit_every_duration"`
		Burst              int           `mapstructure:"burst"`
		LakefsBaseURL      string        `mapstructure:"lakefs_base_url"`
	}
	UI struct {
		Snippets []struct {
			ID   string `mapstructure:"id"`
			Code string `mapstructure:"code"`
		} `mapstructure:"snippets"`
	} `mapstructure:"ui"`
}
