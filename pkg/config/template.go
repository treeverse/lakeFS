package config

import (
	"time"
)

type OIDC struct {
	Enabled        bool `mapstructure:"enabled"`
	IsDefaultLogin bool `mapstructure:"is_default_login"`

	// provider details:
	URL          string `mapstructure:"url"`
	ClientID     string `mapstructure:"client_id"`
	ClientSecret string `mapstructure:"client_secret"`

	// configure the OIDC authentication flow:
	CallbackBaseURL                  string            `mapstructure:"callback_base_url"`
	AuthorizeEndpointQueryParameters map[string]string `mapstructure:"authorize_endpoint_query_parameters"`

	// configure how users are handled on the lakeFS side:
	ValidateIDTokenClaims  map[string]string `mapstructure:"validate_id_token_claims"`
	DefaultInitialGroups   []string          `mapstructure:"default_initial_groups"`
	InitialGroupsClaimName string            `mapstructure:"initial_groups_claim_name"`
	FriendlyNameClaimName  string            `mapstructure:"friendly_name_claim_name"`
}

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
		// Deprecated: use Postgres struct
		DeprecatedConnectionString SecureString `mapstructure:"connection_string"`
		// Deprecated: use Postgres struct
		DeprecatedMaxOpenConnections int32 `mapstructure:"max_open_connections"`
		// Deprecated: use Postgres struct
		DeprecatedMaxIdleConnections int32 `mapstructure:"max_idle_connections"`
		// Deprecated: use Postgres struct
		DeprecatedConnectionMaxLifetime time.Duration `mapstructure:"connection_max_lifetime"`

		// KVEnabled Development flag to switch between postgres DB and KV store implementations
		KVEnabled bool `mapstructure:"kv_enabled"`
		// DropTables Development flag to delete tables after successful migration to KV
		DropTables bool `mapstructure:"drop_tables"`
		// Type  Name of the KV Store driver DB implementation which is available according to the kv package Drivers function
		Type string `mapstructure:"type"`

		Postgres *struct {
			ConnectionString      SecureString  `mapstructure:"connection_string"`
			MaxOpenConnections    int32         `mapstructure:"max_open_connections"`
			MaxIdleConnections    int32         `mapstructure:"max_idle_connections"`
			ConnectionMaxLifetime time.Duration `mapstructure:"connection_max_lifetime"`
		}

		DynamoDB *struct {
			// The name of the DynamoDB table to be used as KV
			TableName string `mapstructure:"table_name"`

			// Table provisioned throughput parameters, as described in
			// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Limits.html
			ReadCapacityUnits  int64 `mapstructure:"read_capacity_units"`
			WriteCapacityUnits int64 `mapstructure:"write_capacity_units"`

			// Maximal number of items per page during scan operation
			ScanLimit int64 `mapstructure:"scan_limit"`

			// The endpoint URL of the DynamoDB endpoint
			// Can be used to redirect to DynamoDB on AWS, local docker etc.
			Endpoint string `mapstructure:"endpoint"`

			// AWS connection details - region and credentials
			// This will override any such details that are already exist in the system
			// While in general, AWS region and credentials are configured in the system for AWS usage,
			// these can be used to specify fake values, that cna be used to connect to local DynamoDB,
			// in case there are no credentials configured in the system
			// This is a client requirement as described in section 4 in
			// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.DownloadingAndRunning.html
			AwsRegion          string       `mapstructure:"aws_region"`
			AwsAccessKeyID     SecureString `mapstructure:"aws_access_key_id"`
			AwsSecretAccessKey SecureString `mapstructure:"aws_secret_access_key"`
		} `mapstructure:"dynamodb"`
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
			Endpoint        string
			Token           string
			SupportsInvites bool `mapstructure:"supports_invites"`
		}
		LDAP              *LDAP
		OIDC              OIDC
		LogoutRedirectURL string `mapstructure:"logout_redirect_url"`
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
		// Enabled - control serving of embedded UI
		Enabled  bool `mapstructure:"enabled"`
		Snippets []struct {
			ID   string `mapstructure:"id"`
			Code string `mapstructure:"code"`
		} `mapstructure:"snippets"`
	} `mapstructure:"ui"`
}
