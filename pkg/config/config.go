package config

import (
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/mitchellh/go-homedir"
	"github.com/mitchellh/mapstructure"
	"github.com/spf13/viper"
	apiparams "github.com/treeverse/lakefs/pkg/api/params"
	blockparams "github.com/treeverse/lakefs/pkg/block/params"
	"github.com/treeverse/lakefs/pkg/logging"
)

var (
	ErrBadConfiguration      = errors.New("bad configuration")
	ErrBadDomainNames        = fmt.Errorf("%w: domain names are prefixes", ErrBadConfiguration)
	ErrMissingRequiredKeys   = fmt.Errorf("%w: missing required keys", ErrBadConfiguration)
	ErrBadGCPCSEKValue       = fmt.Errorf("value of customer-supplied server side encryption is not a valid %d bytes AES key", gcpAESKeyLength)
	ErrGCPEncryptKeyConflict = errors.New("setting both kms and customer supplied encryption will result failure when reading/writing object")
)

// UseLocalConfiguration set to true will add defaults that enable a lakeFS run
// without any other configuration like DB or blockstore.
const (
	UseLocalConfiguration   = "local-settings"
	QuickstartConfiguration = "quickstart"
)

type OIDC struct {
	// configure how users are handled on the lakeFS side:
	ValidateIDTokenClaims  map[string]string `mapstructure:"validate_id_token_claims"`
	DefaultInitialGroups   []string          `mapstructure:"default_initial_groups"`
	InitialGroupsClaimName string            `mapstructure:"initial_groups_claim_name"`
	FriendlyNameClaimName  string            `mapstructure:"friendly_name_claim_name"`
	PersistFriendlyName    bool              `mapstructure:"persist_friendly_name"`
}

// CookieAuthVerification is related to auth based on a cookie set by an external service
// TODO(isan) consolidate with OIDC
type CookieAuthVerification struct {
	// ValidateIDTokenClaims if set will validate the values (e.g., department: "R&D") exist in the token claims
	ValidateIDTokenClaims map[string]string `mapstructure:"validate_id_token_claims"`
	// DefaultInitialGroups is a list of groups to add to the user on the lakeFS side
	DefaultInitialGroups []string `mapstructure:"default_initial_groups"`
	// InitialGroupsClaimName comma separated list of groups to add to the user on the lakeFS side
	InitialGroupsClaimName string `mapstructure:"initial_groups_claim_name"`
	// FriendlyNameClaimName is the claim name to use as the user's friendly name in places like the UI
	FriendlyNameClaimName string `mapstructure:"friendly_name_claim_name"`
	// ExternalUserIDClaimName is the claim name to use as the user identifier with an IDP
	ExternalUserIDClaimName string `mapstructure:"external_user_id_claim_name"`
	// AuthSource tag each user with label of the IDP
	AuthSource string `mapstructure:"auth_source"`
	// PersistFriendlyName should we persist the friendly name in the KV store
	PersistFriendlyName bool `mapstructure:"persist_friendly_name"`
}

// S3AuthInfo holds S3-style authentication.
type S3AuthInfo struct {
	CredentialsFile string `mapstructure:"credentials_file"`
	Profile         string
	Credentials     *struct {
		AccessKeyID     SecureString `mapstructure:"access_key_id"`
		SecretAccessKey SecureString `mapstructure:"secret_access_key"`
		SessionToken    SecureString `mapstructure:"session_token"`
	}
}

// Config - Output struct of configuration, used to validate.  If you read a key using a viper accessor
// rather than accessing a field of this struct, that key will *not* be validated.  So don't
// do that.
type Config struct {
	ListenAddress string `mapstructure:"listen_address"`
	TLS           struct {
		Enabled  bool   `mapstructure:"enabled"`
		CertFile string `mapstructure:"cert_file"`
		KeyFile  string `mapstructure:"key_file"`
	} `mapstructure:"tls"`

	Actions struct {
		// ActionsEnabled set to false will block any hook execution
		Enabled bool `mapstructure:"enabled"`
		Lua     struct {
			NetHTTPEnabled bool `mapstructure:"net_http_enabled"`
		} `mapstructure:"lua"`
		Env struct {
			Enabled bool   `mapstructure:"enabled"`
			Prefix  string `mapstructure:"prefix"`
		} `mapstructure:"env"`
	} `mapstructure:"actions"`

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
		// DropTables Development flag to delete tables after successful migration to KV
		DropTables bool `mapstructure:"drop_tables"`
		// Type Name of the KV Store driver DB implementation which is available according to the kv package Drivers function
		Type string `mapstructure:"type" validate:"required"`

		Local *struct {
			// Path - Local directory path to store the DB files
			Path string `mapstructure:"path"`
			// SyncWrites - Sync ensures data written to disk on each write instead of mem cache
			SyncWrites bool `mapstructure:"sync_writes"`
			// PrefetchSize - Number of elements to prefetch while iterating
			PrefetchSize int `mapstructure:"prefetch_size"`
			// EnableLogging - Enable store and badger (trace only) logging
			EnableLogging bool `mapstructure:"enable_logging"`
		} `mapstructure:"local"`

		Postgres *struct {
			ConnectionString      SecureString  `mapstructure:"connection_string"`
			MaxOpenConnections    int32         `mapstructure:"max_open_connections"`
			MaxIdleConnections    int32         `mapstructure:"max_idle_connections"`
			ConnectionMaxLifetime time.Duration `mapstructure:"connection_max_lifetime"`
			ScanPageSize          int           `mapstructure:"scan_page_size"`
			Metrics               bool          `mapstructure:"metrics"`
		}

		DynamoDB *struct {
			// The name of the DynamoDB table to be used as KV
			TableName string `mapstructure:"table_name"`

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
			AwsProfile         string       `mapstructure:"aws_profile"`
			AwsAccessKeyID     SecureString `mapstructure:"aws_access_key_id"`
			AwsSecretAccessKey SecureString `mapstructure:"aws_secret_access_key"`

			// HealthCheckInterval - Interval to run health check for the DynamoDB instance
			// Won't run when is equal or less than 0.
			HealthCheckInterval time.Duration `mapstructure:"health_check_interval"`

			// MaxAttempts - Specifies the maximum number attempts to make on a request.
			MaxAttempts int `mapstructure:"max_attempts"`

			// Maximum amount of connections to DDB. 0 means no limit.
			MaxConnections int `mapstructure:"max_connections"`
		} `mapstructure:"dynamodb"`

		CosmosDB *struct {
			Key        SecureString `mapstructure:"key"`
			Endpoint   string       `mapstructure:"endpoint"`
			Database   string       `mapstructure:"database"`
			Container  string       `mapstructure:"container"`
			Throughput int32        `mapstructure:"throughput"`
			Autoscale  bool         `mapstructure:"autoscale"`
		} `mapstructure:"cosmosdb"`
	}

	Auth struct {
		Cache struct {
			Enabled bool          `mapstructure:"enabled"`
			Size    int           `mapstructure:"size"`
			TTL     time.Duration `mapstructure:"ttl"`
			Jitter  time.Duration `mapstructure:"jitter"`
		} `mapstructure:"cache"`
		Encrypt struct {
			SecretKey SecureString `mapstructure:"secret_key" validate:"required"`
		} `mapstructure:"encrypt"`
		API struct {
			// Endpoint for authorization operations
			Endpoint           string        `mapstructure:"endpoint"`
			Token              SecureString  `mapstructure:"token"`
			SupportsInvites    bool          `mapstructure:"supports_invites"`
			HealthCheckTimeout time.Duration `mapstructure:"health_check_timeout"`
			SkipHealthCheck    bool          `mapstructure:"skip_health_check"`
		} `mapstructure:"api"`
		AuthenticationAPI struct {
			// Endpoint for authentication operations
			Endpoint string `mapstructure:"endpoint"`
			// ExternalPrincipalAuth configuration related external principals
			ExternalPrincipalsEnabled bool `mapstructure:"external_principals_enabled"`
		} `mapstructure:"authentication_api"`
		RemoteAuthenticator struct {
			// Enabled if set true will enable remote authentication
			Enabled bool `mapstructure:"enabled"`
			// Endpoint URL of the remote authentication service (e.g. https://my-auth.example.com/auth)
			Endpoint string `mapstructure:"endpoint"`
			// DefaultUserGroup is the default group for the users authenticated by the remote service
			DefaultUserGroup string `mapstructure:"default_user_group"`
			// RequestTimeout timeout for remote authentication requests
			RequestTimeout time.Duration `mapstructure:"request_timeout"`
		} `mapstructure:"remote_authenticator"`
		OIDC                   OIDC                   `mapstructure:"oidc"`
		CookieAuthVerification CookieAuthVerification `mapstructure:"cookie_auth_verification"`
		// LogoutRedirectURL is the URL on which to mount the
		// server-side logout.
		LogoutRedirectURL string        `mapstructure:"logout_redirect_url"`
		LoginDuration     time.Duration `mapstructure:"login_duration"`
		LoginMaxDuration  time.Duration `mapstructure:"login_max_duration"`
		UIConfig          struct {
			RBAC               string   `mapstructure:"rbac"`
			LoginURL           string   `mapstructure:"login_url"`
			LoginFailedMessage string   `mapstructure:"login_failed_message"`
			FallbackLoginURL   *string  `mapstructure:"fallback_login_url"`
			FallbackLoginLabel *string  `mapstructure:"fallback_login_label"`
			LoginCookieNames   []string `mapstructure:"login_cookie_names"`
			LogoutURL          string   `mapstructure:"logout_url"`
		} `mapstructure:"ui_config"`
	} `mapstructure:"auth"`
	Blockstore struct {
		Signing struct {
			SecretKey SecureString `mapstructure:"secret_key" validate:"required"`
		} `mapstructure:"signing"`
		Type                   string  `mapstructure:"type" validate:"required"`
		DefaultNamespacePrefix *string `mapstructure:"default_namespace_prefix"`
		Local                  *struct {
			Path                    string   `mapstructure:"path"`
			ImportEnabled           bool     `mapstructure:"import_enabled"`
			ImportHidden            bool     `mapstructure:"import_hidden"`
			AllowedExternalPrefixes []string `mapstructure:"allowed_external_prefixes"`
		} `mapstructure:"local"`
		S3 *struct {
			S3AuthInfo                    `mapstructure:",squash"`
			Region                        string        `mapstructure:"region"`
			Endpoint                      string        `mapstructure:"endpoint"`
			MaxRetries                    int           `mapstructure:"max_retries"`
			ForcePathStyle                bool          `mapstructure:"force_path_style"`
			DiscoverBucketRegion          bool          `mapstructure:"discover_bucket_region"`
			SkipVerifyCertificateTestOnly bool          `mapstructure:"skip_verify_certificate_test_only"`
			ServerSideEncryption          string        `mapstructure:"server_side_encryption"`
			ServerSideEncryptionKmsKeyID  string        `mapstructure:"server_side_encryption_kms_key_id"`
			PreSignedExpiry               time.Duration `mapstructure:"pre_signed_expiry"`
			DisablePreSigned              bool          `mapstructure:"disable_pre_signed"`
			DisablePreSignedUI            bool          `mapstructure:"disable_pre_signed_ui"`
			DisablePreSignedMultipart     bool          `mapstructure:"disable_pre_signed_multipart"`
			ClientLogRetries              bool          `mapstructure:"client_log_retries"`
			ClientLogRequest              bool          `mapstructure:"client_log_request"`
			WebIdentity                   *struct {
				SessionDuration     time.Duration `mapstructure:"session_duration"`
				SessionExpiryWindow time.Duration `mapstructure:"session_expiry_window"`
			} `mapstructure:"web_identity"`
		} `mapstructure:"s3"`
		Azure *struct {
			TryTimeout       time.Duration `mapstructure:"try_timeout"`
			StorageAccount   string        `mapstructure:"storage_account"`
			StorageAccessKey string        `mapstructure:"storage_access_key"`
			// Deprecated: Value ignored
			AuthMethod         string        `mapstructure:"auth_method"`
			PreSignedExpiry    time.Duration `mapstructure:"pre_signed_expiry"`
			DisablePreSigned   bool          `mapstructure:"disable_pre_signed"`
			DisablePreSignedUI bool          `mapstructure:"disable_pre_signed_ui"`
			// Deprecated: Value ignored
			ChinaCloudDeprecated bool   `mapstructure:"china_cloud"`
			TestEndpointURL      string `mapstructure:"test_endpoint_url"`
			// Domain by default points to Azure default domain blob.core.windows.net, can be set to other Azure domains (China/Gov)
			Domain string `mapstructure:"domain"`
		} `mapstructure:"azure"`
		GS *struct {
			S3Endpoint                           string        `mapstructure:"s3_endpoint"`
			CredentialsFile                      string        `mapstructure:"credentials_file"`
			CredentialsJSON                      string        `mapstructure:"credentials_json"`
			PreSignedExpiry                      time.Duration `mapstructure:"pre_signed_expiry"`
			DisablePreSigned                     bool          `mapstructure:"disable_pre_signed"`
			DisablePreSignedUI                   bool          `mapstructure:"disable_pre_signed_ui"`
			ServerSideEncryptionCustomerSupplied string        `mapstructure:"server_side_encryption_customer_supplied"`
			ServerSideEncryptionKmsKeyID         string        `mapstructure:"server_side_encryption_kms_key_id"`
		} `mapstructure:"gs"`
	} `mapstructure:"blockstore"`
	Committed struct {
		LocalCache struct {
			SizeBytes             int64   `mapstructure:"size_bytes"`
			Dir                   string  `mapstructure:"dir"`
			MaxUploadersPerWriter int     `mapstructure:"max_uploaders_per_writer"`
			RangeProportion       float64 `mapstructure:"range_proportion"`
			MetaRangeProportion   float64 `mapstructure:"metarange_proportion"`
		} `mapstructure:"local_cache"`
		BlockStoragePrefix string `mapstructure:"block_storage_prefix"`
		Permanent          struct {
			MinRangeSizeBytes      uint64  `mapstructure:"min_range_size_bytes"`
			MaxRangeSizeBytes      uint64  `mapstructure:"max_range_size_bytes"`
			RangeRaggednessEntries float64 `mapstructure:"range_raggedness_entries"`
		} `mapstructure:"permanent"`
		SSTable struct {
			Memory struct {
				CacheSizeBytes int64 `mapstructure:"cache_size_bytes"`
			} `mapstructure:"memory"`
		} `mapstructure:"sstable"`
	} `mapstructure:"committed"`
	UGC struct {
		PrepareMaxFileSize int64         `mapstructure:"prepare_max_file_size"`
		PrepareInterval    time.Duration `mapstructure:"prepare_interval"`
	} `mapstructure:"ugc"`
	Graveler struct {
		EnsureReadableRootNamespace bool `mapstructure:"ensure_readable_root_namespace"`
		BatchDBIOTransactionMarkers bool `mapstructure:"batch_dbio_transaction_markers"`
		CompactionSensorThreshold   int  `mapstructure:"compaction_sensor_threshold"`
		RepositoryCache             struct {
			Size   int           `mapstructure:"size"`
			Expiry time.Duration `mapstructure:"expiry"`
			Jitter time.Duration `mapstructure:"jitter"`
		} `mapstructure:"repository_cache"`
		CommitCache struct {
			Size   int           `mapstructure:"size"`
			Expiry time.Duration `mapstructure:"expiry"`
			Jitter time.Duration `mapstructure:"jitter"`
		} `mapstructure:"commit_cache"`
		Background struct {
			RateLimit int `mapstructure:"rate_limit"`
		} `mapstructure:"background"`
		MaxBatchDelay time.Duration `mapstructure:"max_batch_delay"`
	} `mapstructure:"graveler"`
	Gateways struct {
		S3 struct {
			DomainNames       Strings `mapstructure:"domain_name"`
			Region            string  `mapstructure:"region"`
			FallbackURL       string  `mapstructure:"fallback_url"`
			VerifyUnsupported bool    `mapstructure:"verify_unsupported"`
		} `mapstructure:"s3"`
	}
	Stats struct {
		Enabled       bool          `mapstructure:"enabled"`
		Address       string        `mapstructure:"address"`
		FlushInterval time.Duration `mapstructure:"flush_interval"`
		FlushSize     int           `mapstructure:"flush_size"`
		Extended      bool          `mapstructure:"extended"`
	} `mapstructure:"stats"`
	EmailSubscription struct {
		Enabled bool `mapstructure:"enabled"`
	} `mapstructure:"email_subscription"`
	Installation struct {
		FixedID         string       `mapstructure:"fixed_id"`
		UserName        string       `mapstructure:"user_name"`
		AccessKeyID     SecureString `mapstructure:"access_key_id"`
		SecretAccessKey SecureString `mapstructure:"secret_access_key"`
	} `mapstructure:"installation"`
	Security struct {
		CheckLatestVersion      bool          `mapstructure:"check_latest_version"`
		CheckLatestVersionCache time.Duration `mapstructure:"check_latest_version_cache"`
		AuditCheckInterval      time.Duration `mapstructure:"audit_check_interval"`
		AuditCheckURL           string        `mapstructure:"audit_check_url"`
	} `mapstructure:"security"`
	UI struct {
		// Enabled - control serving of embedded UI
		Enabled  bool `mapstructure:"enabled"`
		Snippets []struct {
			ID   string `mapstructure:"id"`
			Code string `mapstructure:"code"`
		} `mapstructure:"snippets"`
	} `mapstructure:"ui"`
	UsageReport struct {
		Enabled       bool          `mapstructure:"enabled"`
		FlushInterval time.Duration `mapstructure:"flush_interval"`
	} `mapstructure:"usage_report"`
}

func NewConfig(cfgType string) (*Config, error) {
	return newConfig(cfgType)
}

func newConfig(cfgType string) (*Config, error) {
	c := &Config{}

	// Inform viper of all expected fields.  Otherwise, it fails to deserialize from the
	// environment.
	keys := GetStructKeys(reflect.TypeOf(c), "mapstructure", "squash")
	for _, key := range keys {
		viper.SetDefault(key, nil)
	}
	setDefaults(cfgType)

	err := Unmarshal(c)
	if err != nil {
		return nil, err
	}

	err = c.validateDomainNames()
	if err != nil {
		return nil, err
	}

	// setup logging package
	logging.SetOutputFormat(c.Logging.Format)
	err = logging.SetOutputs(c.Logging.Output, c.Logging.FileMaxSizeMB, c.Logging.FilesKeep)
	if err != nil {
		return nil, err
	}
	logging.SetLevel(c.Logging.Level)
	return c, nil
}

func Unmarshal(c *Config) error {
	return viper.UnmarshalExact(&c,
		viper.DecodeHook(
			mapstructure.ComposeDecodeHookFunc(
				DecodeStrings, mapstructure.StringToTimeDurationHookFunc())))
}

func stringReverse(s string) string {
	chars := []rune(s)
	for i := 0; i < len(chars)/2; i++ {
		j := len(chars) - 1 - i
		chars[i], chars[j] = chars[j], chars[i]
	}
	return string(chars)
}

func (c *Config) validateDomainNames() error {
	domainStrings := c.Gateways.S3.DomainNames
	domainNames := make([]string, len(domainStrings))
	copy(domainNames, domainStrings)
	for i, d := range domainNames {
		domainNames[i] = stringReverse(d)
	}
	sort.Strings(domainNames)
	for i, d := range domainNames {
		domainNames[i] = stringReverse(d)
	}
	for i := 0; i < len(domainNames)-1; i++ {
		if strings.HasSuffix(domainNames[i+1], "."+domainNames[i]) {
			return fmt.Errorf("%w: %s, %s", ErrBadDomainNames, domainNames[i], domainNames[i+1])
		}
	}
	return nil
}

func (c *Config) Validate() error {
	missingKeys := ValidateMissingRequiredKeys(c, "mapstructure", "squash")
	if len(missingKeys) > 0 {
		return fmt.Errorf("%w: %v", ErrMissingRequiredKeys, missingKeys)
	}
	return nil
}

func (c *Config) BlockstoreType() string {
	return c.Blockstore.Type
}

func (c *Config) BlockstoreS3Params() (blockparams.S3, error) {
	var webIdentity *blockparams.S3WebIdentity
	if c.Blockstore.S3.WebIdentity != nil {
		webIdentity = &blockparams.S3WebIdentity{
			SessionDuration:     c.Blockstore.S3.WebIdentity.SessionDuration,
			SessionExpiryWindow: c.Blockstore.S3.WebIdentity.SessionExpiryWindow,
		}
	}

	var creds blockparams.S3Credentials
	if c.Blockstore.S3.Credentials != nil {
		creds.AccessKeyID = c.Blockstore.S3.Credentials.AccessKeyID.SecureValue()
		creds.SecretAccessKey = c.Blockstore.S3.Credentials.SecretAccessKey.SecureValue()
		creds.SessionToken = c.Blockstore.S3.Credentials.SessionToken.SecureValue()
	}

	return blockparams.S3{
		Region:                        c.Blockstore.S3.Region,
		Profile:                       c.Blockstore.S3.Profile,
		CredentialsFile:               c.Blockstore.S3.CredentialsFile,
		Credentials:                   creds,
		MaxRetries:                    c.Blockstore.S3.MaxRetries,
		Endpoint:                      c.Blockstore.S3.Endpoint,
		ForcePathStyle:                c.Blockstore.S3.ForcePathStyle,
		DiscoverBucketRegion:          c.Blockstore.S3.DiscoverBucketRegion,
		SkipVerifyCertificateTestOnly: c.Blockstore.S3.SkipVerifyCertificateTestOnly,
		ServerSideEncryption:          c.Blockstore.S3.ServerSideEncryption,
		ServerSideEncryptionKmsKeyID:  c.Blockstore.S3.ServerSideEncryptionKmsKeyID,
		PreSignedExpiry:               c.Blockstore.S3.PreSignedExpiry,
		DisablePreSigned:              c.Blockstore.S3.DisablePreSigned,
		DisablePreSignedUI:            c.Blockstore.S3.DisablePreSignedUI,
		DisablePreSignedMultipart:     c.Blockstore.S3.DisablePreSignedMultipart,
		ClientLogRetries:              c.Blockstore.S3.ClientLogRetries,
		ClientLogRequest:              c.Blockstore.S3.ClientLogRequest,
		WebIdentity:                   webIdentity,
	}, nil
}

func (c *Config) BlockstoreLocalParams() (blockparams.Local, error) {
	localPath := c.Blockstore.Local.Path
	path, err := homedir.Expand(localPath)
	if err != nil {
		return blockparams.Local{}, fmt.Errorf("parse blockstore location URI %s: %w", localPath, err)
	}

	params := blockparams.Local(*c.Blockstore.Local)
	params.Path = path
	return params, nil
}

const (
	gcpAESKeyLength = 32
)

func (c *Config) BlockstoreGSParams() (blockparams.GS, error) {
	var customerSuppliedKey []byte = nil
	if c.Blockstore.GS.ServerSideEncryptionCustomerSupplied != "" {
		v, err := hex.DecodeString(c.Blockstore.GS.ServerSideEncryptionCustomerSupplied)
		if err != nil {
			return blockparams.GS{}, err
		}
		if len(v) != gcpAESKeyLength {
			return blockparams.GS{}, ErrBadGCPCSEKValue
		}
		customerSuppliedKey = v
		if c.Blockstore.GS.ServerSideEncryptionKmsKeyID != "" {
			return blockparams.GS{}, ErrGCPEncryptKeyConflict
		}
	}

	credPath, err := homedir.Expand(c.Blockstore.GS.CredentialsFile)
	if err != nil {
		return blockparams.GS{}, fmt.Errorf("parse GS credentials path '%s': %w", c.Blockstore.GS.CredentialsFile, err)
	}
	return blockparams.GS{
		CredentialsFile:                      credPath,
		CredentialsJSON:                      c.Blockstore.GS.CredentialsJSON,
		PreSignedExpiry:                      c.Blockstore.GS.PreSignedExpiry,
		DisablePreSigned:                     c.Blockstore.GS.DisablePreSigned,
		DisablePreSignedUI:                   c.Blockstore.GS.DisablePreSignedUI,
		ServerSideEncryptionCustomerSupplied: customerSuppliedKey,
		ServerSideEncryptionKmsKeyID:         c.Blockstore.GS.ServerSideEncryptionKmsKeyID,
	}, nil
}

func (c *Config) BlockstoreAzureParams() (blockparams.Azure, error) {
	if c.Blockstore.Azure.AuthMethod != "" {
		logging.ContextUnavailable().Warn("blockstore.azure.auth_method is deprecated. Value is no longer used.")
	}
	if c.Blockstore.Azure.ChinaCloudDeprecated {
		logging.ContextUnavailable().Warn("blockstore.azure.china_cloud is deprecated. Value is no longer used. Please pass Domain = 'blob.core.chinacloudapi.cn'")
		c.Blockstore.Azure.Domain = "blob.core.chinacloudapi.cn"
	}
	return blockparams.Azure{
		StorageAccount:     c.Blockstore.Azure.StorageAccount,
		StorageAccessKey:   c.Blockstore.Azure.StorageAccessKey,
		TryTimeout:         c.Blockstore.Azure.TryTimeout,
		PreSignedExpiry:    c.Blockstore.Azure.PreSignedExpiry,
		TestEndpointURL:    c.Blockstore.Azure.TestEndpointURL,
		Domain:             c.Blockstore.Azure.Domain,
		DisablePreSigned:   c.Blockstore.Azure.DisablePreSigned,
		DisablePreSignedUI: c.Blockstore.Azure.DisablePreSignedUI,
	}, nil
}

const (
	AuthRBACSimplified = "simplified"
	AuthRBACExternal   = "external"
	AuthRBACInternal   = "internal"
)

func (c *Config) IsAuthUISimplified() bool {
	return c.Auth.UIConfig.RBAC == AuthRBACSimplified
}

func (c *Config) IsAuthenticationTypeAPI() bool {
	return c.Auth.AuthenticationAPI.Endpoint != ""
}
func (c *Config) IsAuthTypeAPI() bool {
	return c.Auth.API.Endpoint != ""
}
func (c *Config) IsExternalPrincipalsEnabled() bool {
	// IsAuthTypeAPI must be true since the local auth service doesnt support external principals
	// ExternalPrincipalsEnabled indicates that the remote auth service enables external principals support since its optional extension
	return c.IsAuthTypeAPI() && c.Auth.AuthenticationAPI.ExternalPrincipalsEnabled
}

func (c *Config) UISnippets() []apiparams.CodeSnippet {
	snippets := make([]apiparams.CodeSnippet, 0, len(c.UI.Snippets))
	for _, item := range c.UI.Snippets {
		snippets = append(snippets, apiparams.CodeSnippet{
			ID:   item.ID,
			Code: item.Code,
		})
	}
	return snippets
}
