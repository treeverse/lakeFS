package config

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/mitchellh/go-homedir"
	"github.com/mitchellh/mapstructure"
	"github.com/spf13/viper"
	apiparams "github.com/treeverse/lakefs/pkg/api/params"
	"github.com/treeverse/lakefs/pkg/block"
	blockparams "github.com/treeverse/lakefs/pkg/block/params"
	"github.com/treeverse/lakefs/pkg/graveler/committed"
	kvparams "github.com/treeverse/lakefs/pkg/kv/params"
	"github.com/treeverse/lakefs/pkg/logging"
	pyramidparams "github.com/treeverse/lakefs/pkg/pyramid/params"
	"go.uber.org/ratelimit"
)

var (
	ErrBadConfiguration    = errors.New("bad configuration")
	ErrMissingSecretKey    = fmt.Errorf("%w: auth.encrypt.secret_key cannot be empty", ErrBadConfiguration)
	ErrInvalidProportion   = fmt.Errorf("%w: total proportion isn't 1.0", ErrBadConfiguration)
	ErrBadDomainNames      = fmt.Errorf("%w: domain names are prefixes", ErrBadConfiguration)
	ErrMissingRequiredKeys = fmt.Errorf("%w: missing required keys", ErrBadConfiguration)
)

// UseLocalConfiguration set to true will add defaults that enable a lakeFS run
// without any other configuration like DB or blockstore.
const UseLocalConfiguration = "local-settings"

type OIDC struct {
	// configure how users are handled on the lakeFS side:
	ValidateIDTokenClaims  map[string]string `mapstructure:"validate_id_token_claims"`
	DefaultInitialGroups   []string          `mapstructure:"default_initial_groups"`
	InitialGroupsClaimName string            `mapstructure:"initial_groups_claim_name"`
	FriendlyNameClaimName  string            `mapstructure:"friendly_name_claim_name"`
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

// Config - Output struct of configuration, used to validate.  If you read a key using a viper accessor
// rather than accessing a field of this struct, that key will *not* be validated.  So don't
// do that.
type Config struct {
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
			ConnectionString      string        `mapstructure:"connection_string"`
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
			AwsRegion          string `mapstructure:"aws_region"`
			AwsProfile         string `mapstructure:"aws_profile"`
			AwsAccessKeyID     string `mapstructure:"aws_access_key_id"`
			AwsSecretAccessKey string `mapstructure:"aws_secret_access_key"`

			// HealthCheckInterval - Interval to run health check for the DynamoDB instance
			// Won't run when is equal or less than 0.
			HealthCheckInterval time.Duration `mapstructure:"health_check_interval"`
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
		RemoteAuthenticator struct {
			// Enabled if set true will enable remote authentication
			Enabled bool `mapstructure:"enabled"`
			// BaseURL is the base URL of the remote authentication service (e.g. https://my-auth.example.com)
			BaseURL string `mapstructure:"base_url"`
			// AuthEndpoint is the endpoint to authenticate users (e.g. /auth)
			AuthEndpoint string `mapstructure:"auth_endpoint"`
			// DefaultUserGroup is the default group for the users authenticated by the remote service
			DefaultUserGroup string `mapstructure:"default_user_group"`
			// RequestTimeout timeout for remote authentication requests
			RequestTimeout time.Duration `mapstructure:"request_timeout"`
		} `mapstructure:"remote_authenticator"`
		OIDC OIDC `mapstructure:"oidc"`
		// LogoutRedirectURL is the URL on which to mount the
		// server-side logout.
		LogoutRedirectURL string        `mapstructure:"logout_redirect_url"`
		LoginDuration     time.Duration `mapstructure:"login_duration"`
		UIConfig          struct {
			RBAC               string   `mapstructure:"rbac"`
			LoginURL           string   `mapstructure:"login_url"`
			LoginFailedMessage string   `mapstructure:"login_failed_message"`
			FallbackLoginURL   *string  `mapstructure:"fallback_login_url"`
			FallbackLoginLabel *string  `mapstructure:"fallback_login_label"`
			LoginCookieNames   []string `mapstructure:"login_cookie_names"`
			LogoutURL          string   `mapstructure:"logout_url"`
		} `mapstructure:"ui_config"`
	}
	Blockstore struct {
		Type                   string `mapstructure:"type" validate:"required"`
		DefaultNamespacePrefix string `mapstructure:"default_namespace_prefix"`
		Local                  *struct {
			Path                    string   `mapstructure:"path"`
			ImportEnabled           bool     `mapstructure:"import_enabled"`
			ImportHidden            bool     `mapstructure:"import_hidden"`
			AllowedExternalPrefixes []string `mapstructure:"allowed_external_prefixes"`
		}
		S3 *struct {
			S3AuthInfo                    `mapstructure:",squash"`
			Region                        string        `mapstructure:"region"`
			Endpoint                      string        `mapstructure:"endpoint"`
			StreamingChunkSize            int           `mapstructure:"streaming_chunk_size"`
			StreamingChunkTimeout         time.Duration `mapstructure:"streaming_chunk_timeout"`
			MaxRetries                    int           `mapstructure:"max_retries"`
			ForcePathStyle                bool          `mapstructure:"force_path_style"`
			DiscoverBucketRegion          bool          `mapstructure:"discover_bucket_region"`
			SkipVerifyCertificateTestOnly bool          `mapstructure:"skip_verify_certificate_test_only"`
			ServerSideEncryption          string        `mapstructure:"server_side_encryption"`
			ServerSideEncryptionKmsKeyID  string        `mapstructure:"server_side_encryption_kms_key_id"`
			PreSignedExpiry               time.Duration `mapstructure:"pre_signed_expiry"`
			DisablePreSigned              bool          `mapstructure:"disable_pre_signed"`
		} `mapstructure:"s3"`
		Azure *struct {
			TryTimeout       time.Duration `mapstructure:"try_timeout"`
			StorageAccount   string        `mapstructure:"storage_account"`
			StorageAccessKey string        `mapstructure:"storage_access_key"`
			// Deprecated: Value ignored
			AuthMethod       string        `mapstructure:"auth_method"`
			PreSignedExpiry  time.Duration `mapstructure:"pre_signed_expiry"`
			DisablePreSigned bool          `mapstructure:"disable_pre_signed"`
		} `mapstructure:"azure"`
		GS *struct {
			S3Endpoint       string        `mapstructure:"s3_endpoint"`
			CredentialsFile  string        `mapstructure:"credentials_file"`
			CredentialsJSON  string        `mapstructure:"credentials_json"`
			PreSignedExpiry  time.Duration `mapstructure:"pre_signed_expiry"`
			DisablePreSigned bool          `mapstructure:"disable_pre_signed"`
		} `mapstructure:"gs"`
	}
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
		}
	}
	Graveler struct {
		RepositoryCache struct {
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
	} `mapstructure:"graveler"`
	Gateways struct {
		S3 struct {
			DomainNames Strings `mapstructure:"domain_name"`
			Region      string  `mapstructure:"region"`
			FallbackURL string  `mapstructure:"fallback_url"`
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
		FixedID string `mapstructure:"fixed_id"`
	} `mapstructure:"installation"`
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
	} `mapstructure:"email"`
	UI struct {
		// Enabled - control serving of embedded UI
		Enabled  bool `mapstructure:"enabled"`
		Snippets []struct {
			ID   string `mapstructure:"id"`
			Code string `mapstructure:"code"`
		} `mapstructure:"snippets"`
	} `mapstructure:"ui"`
}

func NewConfig() (*Config, error) {
	return newConfig(false)
}

func NewLocalConfig() (*Config, error) {
	return newConfig(true)
}

func newConfig(local bool) (*Config, error) {
	c := &Config{}

	// Inform viper of all expected fields.  Otherwise, it fails to deserialize from the
	// environment.
	keys := GetStructKeys(reflect.TypeOf(c), "mapstructure", "squash")
	for _, key := range keys {
		viper.SetDefault(key, nil)
	}
	setDefaults(local)

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
	logging.SetOutputs(c.Logging.Output, c.Logging.FileMaxSizeMB, c.Logging.FilesKeep)
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

func (c *Config) DatabaseParams() (kvparams.Config, error) {
	p := kvparams.Config{
		Type: c.Database.Type,
	}
	if c.Database.Local != nil {
		localPath, err := homedir.Expand(c.Database.Local.Path)
		if err != nil {
			return kvparams.Config{}, fmt.Errorf("parse database local path '%s': %w", c.Database.Local.Path, err)
		}
		p.Local = &kvparams.Local{
			Path:         localPath,
			PrefetchSize: c.Database.Local.PrefetchSize,
		}
	}

	if c.Database.Postgres != nil {
		p.Postgres = &kvparams.Postgres{
			ConnectionString:      c.Database.Postgres.ConnectionString,
			MaxIdleConnections:    c.Database.Postgres.MaxIdleConnections,
			MaxOpenConnections:    c.Database.Postgres.MaxOpenConnections,
			ConnectionMaxLifetime: c.Database.Postgres.ConnectionMaxLifetime,
		}
	}

	if c.Database.DynamoDB != nil {
		p.DynamoDB = &kvparams.DynamoDB{
			TableName:           c.Database.DynamoDB.TableName,
			ScanLimit:           c.Database.DynamoDB.ScanLimit,
			Endpoint:            c.Database.DynamoDB.Endpoint,
			AwsRegion:           c.Database.DynamoDB.AwsRegion,
			AwsProfile:          c.Database.DynamoDB.AwsProfile,
			AwsAccessKeyID:      c.Database.DynamoDB.AwsAccessKeyID,
			AwsSecretAccessKey:  c.Database.DynamoDB.AwsSecretAccessKey,
			HealthCheckInterval: c.Database.DynamoDB.HealthCheckInterval,
		}
	}
	return p, nil
}

func (c *Config) GetAwsConfig() *aws.Config {
	logger := logging.Default().WithField("sdk", "aws")
	cfg := &aws.Config{
		Logger: &logging.AWSAdapter{Logger: logger},
	}
	if c.Blockstore.S3.Region != "" {
		cfg.Region = aws.String(c.Blockstore.S3.Region)
	}
	level := strings.ToLower(logging.Level())
	if level == "trace" {
		cfg.LogLevel = aws.LogLevel(aws.LogDebugWithRequestRetries | aws.LogDebugWithRequestErrors)
	}
	if c.Blockstore.S3.Profile != "" || c.Blockstore.S3.CredentialsFile != "" {
		cfg.Credentials = credentials.NewSharedCredentials(
			c.Blockstore.S3.CredentialsFile,
			c.Blockstore.S3.Profile,
		)
	}
	if c.Blockstore.S3.Credentials != nil {
		secretAccessKey := c.Blockstore.S3.Credentials.SecretAccessKey
		if secretAccessKey == "" {
			logging.Default().Warn("blockstore.s3.credentials.access_secret_key is deprecated. Use instead: blockstore.s3.credentials.secret_access_key.")
			secretAccessKey = c.Blockstore.S3.Credentials.AccessSecretKey
		}
		cfg.Credentials = credentials.NewStaticCredentials(
			c.Blockstore.S3.Credentials.AccessKeyID.SecureValue(),
			secretAccessKey.SecureValue(),
			c.Blockstore.S3.Credentials.SessionToken.SecureValue(),
		)
	}

	s3Endpoint := c.Blockstore.S3.Endpoint
	if len(s3Endpoint) > 0 {
		cfg = cfg.WithEndpoint(s3Endpoint)
	}
	s3ForcePathStyle := c.Blockstore.S3.ForcePathStyle
	if s3ForcePathStyle {
		cfg = cfg.WithS3ForcePathStyle(true)
	}
	cfg = cfg.WithMaxRetries(c.Blockstore.S3.MaxRetries)
	return cfg
}

func (c *Config) BlockstoreType() string {
	return c.Blockstore.Type
}

func (c *Config) BlockstoreS3Params() (blockparams.S3, error) {
	return blockparams.S3{
		AwsConfig:                     c.GetAwsConfig(),
		StreamingChunkSize:            c.Blockstore.S3.StreamingChunkSize,
		StreamingChunkTimeout:         c.Blockstore.S3.StreamingChunkTimeout,
		DiscoverBucketRegion:          c.Blockstore.S3.DiscoverBucketRegion,
		SkipVerifyCertificateTestOnly: c.Blockstore.S3.SkipVerifyCertificateTestOnly,
		ServerSideEncryption:          c.Blockstore.S3.ServerSideEncryption,
		ServerSideEncryptionKmsKeyID:  c.Blockstore.S3.ServerSideEncryptionKmsKeyID,
		PreSignedExpiry:               c.Blockstore.S3.PreSignedExpiry,
		DisablePreSigned:              c.Blockstore.S3.DisablePreSigned,
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

func (c *Config) BlockstoreGSParams() (blockparams.GS, error) {
	return blockparams.GS{
		CredentialsFile: c.Blockstore.GS.CredentialsFile,
		CredentialsJSON: c.Blockstore.GS.CredentialsJSON,
		PreSignedExpiry: c.Blockstore.GS.PreSignedExpiry,
	}, nil
}

func (c *Config) BlockstoreAzureParams() (blockparams.Azure, error) {
	if c.Blockstore.Azure.AuthMethod != "" {
		logging.Default().Warn("blockstore.azure.auth_method is deprecated. Value is no longer used.")
	}
	return blockparams.Azure{
		StorageAccount:   c.Blockstore.Azure.StorageAccount,
		StorageAccessKey: c.Blockstore.Azure.StorageAccessKey,
		TryTimeout:       c.Blockstore.Azure.TryTimeout,
		PreSignedExpiry:  c.Blockstore.Azure.PreSignedExpiry,
	}, nil
}

func (c *Config) AuthEncryptionSecret() []byte {
	secret := c.Auth.Encrypt.SecretKey
	if len(secret) == 0 {
		panic(fmt.Errorf("%w. Please set it to a unique, randomly generated value and store it somewhere safe", ErrMissingSecretKey))
	}
	return []byte(secret)
}

const floatSumTolerance = 1e-6

// GetCommittedTierFSParams returns parameters for building a tierFS.  Caller must separately
// build and populate Adapter.
func (c *Config) GetCommittedTierFSParams(adapter block.Adapter) (*pyramidparams.ExtParams, error) {
	rangePro := c.Committed.LocalCache.RangeProportion
	metaRangePro := c.Committed.LocalCache.MetaRangeProportion

	if math.Abs(rangePro+metaRangePro-1) > floatSumTolerance {
		return nil, fmt.Errorf("range_proportion(%f) and metarange_proportion(%f): %w", rangePro, metaRangePro, ErrInvalidProportion)
	}

	localCacheDir, err := homedir.Expand(c.Committed.LocalCache.Dir)
	if err != nil {
		return nil, fmt.Errorf("expand %s: %w", c.Committed.LocalCache.Dir, err)
	}

	logger := logging.Default().WithField("module", "pyramid")
	return &pyramidparams.ExtParams{
		RangeAllocationProportion:     rangePro,
		MetaRangeAllocationProportion: metaRangePro,
		SharedParams: pyramidparams.SharedParams{
			Logger:             logger,
			Adapter:            adapter,
			BlockStoragePrefix: c.Committed.BlockStoragePrefix,
			Local: pyramidparams.LocalDiskParams{
				BaseDir:             localCacheDir,
				TotalAllocatedBytes: c.Committed.LocalCache.SizeBytes,
			},
			PebbleSSTableCacheSizeBytes: c.Committed.SSTable.Memory.CacheSizeBytes,
		},
	}, nil
}

func (c *Config) CommittedParams() committed.Params {
	return committed.Params{
		MinRangeSizeBytes:          c.Committed.Permanent.MinRangeSizeBytes,
		MaxRangeSizeBytes:          c.Committed.Permanent.MaxRangeSizeBytes,
		RangeSizeEntriesRaggedness: c.Committed.Permanent.RangeRaggednessEntries,
		MaxUploaders:               c.Committed.LocalCache.MaxUploadersPerWriter,
	}
}

func (c *Config) IsAuthTypeAPI() bool {
	return c.Auth.API.Endpoint != ""
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

func (c *Config) NewGravelerBackgroundLimiter() ratelimit.Limiter {
	rateLimit := c.Graveler.Background.RateLimit
	if rateLimit == 0 {
		return ratelimit.NewUnlimited()
	}
	return ratelimit.New(rateLimit)
}
