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
	"github.com/treeverse/lakefs/pkg/auth/email"
	authparams "github.com/treeverse/lakefs/pkg/auth/params"
	"github.com/treeverse/lakefs/pkg/block"
	blockparams "github.com/treeverse/lakefs/pkg/block/params"
	"github.com/treeverse/lakefs/pkg/graveler/committed"
	"github.com/treeverse/lakefs/pkg/graveler/ref"
	kvparams "github.com/treeverse/lakefs/pkg/kv/params"
	"github.com/treeverse/lakefs/pkg/logging"
	pyramidparams "github.com/treeverse/lakefs/pkg/pyramid/params"
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

type Config struct {
	values configuration
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
	keys := GetStructKeys(reflect.TypeOf(c.values), "mapstructure", "squash")
	for _, key := range keys {
		viper.SetDefault(key, nil)
	}

	setDefaults(local)
	setupLogger()

	err := viper.UnmarshalExact(&c.values, viper.DecodeHook(
		mapstructure.ComposeDecodeHookFunc(
			DecodeStrings, mapstructure.StringToTimeDurationHookFunc())))
	if err != nil {
		return nil, err
	}

	err = c.validateDomainNames()
	if err != nil {
		return nil, err
	}

	return c, nil
}

func reverse(s string) string {
	chars := []rune(s)
	for i := 0; i < len(chars)/2; i++ {
		j := len(chars) - 1 - i
		chars[i], chars[j] = chars[j], chars[i]
	}
	return string(chars)
}

func (c *Config) validateDomainNames() error {
	domainStrings := c.GetS3GatewayDomainNames()
	domainNames := make([]string, len(domainStrings))
	copy(domainNames, domainStrings)
	for i, d := range domainNames {
		domainNames[i] = reverse(d)
	}
	sort.Strings(domainNames)
	for i, d := range domainNames {
		domainNames[i] = reverse(d)
	}
	for i := 0; i < len(domainNames)-1; i++ {
		if strings.HasSuffix(domainNames[i+1], "."+domainNames[i]) {
			return fmt.Errorf("%w: %s, %s", ErrBadDomainNames, domainNames[i], domainNames[i+1])
		}
	}
	return nil
}

func (c *Config) Validate() error {
	missingKeys := ValidateMissingRequiredKeys(c.values, "mapstructure", "squash")
	if len(missingKeys) > 0 {
		return fmt.Errorf("%w: %v", ErrMissingRequiredKeys, missingKeys)
	}

	return nil
}

func (c *Config) GetDatabaseType() string {
	return c.values.Database.Type
}

func (c *Config) GetKVParams() (kvparams.KV, error) {
	p := kvparams.KV{
		Type: c.values.Database.Type,
	}
	if c.values.Database.Local != nil {
		localPath, err := homedir.Expand(c.values.Database.Local.Path)
		if err != nil {
			return kvparams.KV{}, fmt.Errorf("parse database local path '%s': %w", c.values.Database.Local.Path, err)
		}
		p.Local = &kvparams.Local{
			Path:         localPath,
			PrefetchSize: c.values.Database.Local.PrefetchSize,
		}
		p.Local.SyncWrites = true
		if c.values.Database.Local.SyncWrites != nil {
			p.Local.SyncWrites = *c.values.Database.Local.SyncWrites
		}
		p.Local.EnableLogging = false
		if c.values.Database.Local.EnableLogging != nil {
			p.Local.EnableLogging = *c.values.Database.Local.EnableLogging
		}
	}

	if c.values.Database.Postgres != nil {
		p.Postgres = &kvparams.Postgres{
			ConnectionString:      c.values.Database.Postgres.ConnectionString.SecureValue(),
			MaxIdleConnections:    c.values.Database.Postgres.MaxIdleConnections,
			MaxOpenConnections:    c.values.Database.Postgres.MaxOpenConnections,
			ConnectionMaxLifetime: c.values.Database.Postgres.ConnectionMaxLifetime,
		}
	}

	if c.values.Database.DynamoDB != nil {
		p.DynamoDB = &kvparams.DynamoDB{
			TableName:          c.values.Database.DynamoDB.TableName,
			ReadCapacityUnits:  c.values.Database.DynamoDB.ReadCapacityUnits,
			WriteCapacityUnits: c.values.Database.DynamoDB.WriteCapacityUnits,
			ScanLimit:          c.values.Database.DynamoDB.ScanLimit,
			Endpoint:           c.values.Database.DynamoDB.Endpoint,
			AwsRegion:          c.values.Database.DynamoDB.AwsRegion,
			AwsProfile:         c.values.Database.DynamoDB.AwsProfile,
			AwsAccessKeyID:     c.values.Database.DynamoDB.AwsAccessKeyID.SecureValue(),
			AwsSecretAccessKey: c.values.Database.DynamoDB.AwsSecretAccessKey.SecureValue(),
		}
	}
	return p, nil
}

func (c *Config) GetLDAPConfiguration() *LDAP {
	return c.values.Auth.LDAP
}

func (c *Config) GetAwsConfig() *aws.Config {
	logger := logging.Default().WithField("sdk", "aws")
	cfg := &aws.Config{
		Region: aws.String(c.values.Blockstore.S3.Region),
		Logger: &logging.AWSAdapter{Logger: logger},
	}
	level := strings.ToLower(logging.Level())
	if level == "trace" {
		cfg.LogLevel = aws.LogLevel(aws.LogDebugWithRequestRetries | aws.LogDebugWithRequestErrors)
	}
	if c.values.Blockstore.S3.Profile != "" || c.values.Blockstore.S3.CredentialsFile != "" {
		cfg.Credentials = credentials.NewSharedCredentials(
			c.values.Blockstore.S3.CredentialsFile,
			c.values.Blockstore.S3.Profile,
		)
	}
	if c.values.Blockstore.S3.Credentials != nil {
		secretAccessKey := c.values.Blockstore.S3.Credentials.SecretAccessKey
		if secretAccessKey == "" {
			logging.Default().Warn("blockstore.s3.credentials.access_secret_key is deprecated. Use instead: blockstore.s3.credentials.secret_access_key.")
			secretAccessKey = c.values.Blockstore.S3.Credentials.AccessSecretKey
		}
		cfg.Credentials = credentials.NewStaticCredentials(
			c.values.Blockstore.S3.Credentials.AccessKeyID.SecureValue(),
			secretAccessKey.SecureValue(),
			c.values.Blockstore.S3.Credentials.SessionToken.SecureValue(),
		)
	}

	s3Endpoint := c.values.Blockstore.S3.Endpoint
	if len(s3Endpoint) > 0 {
		cfg = cfg.WithEndpoint(s3Endpoint)
	}
	s3ForcePathStyle := c.values.Blockstore.S3.ForcePathStyle
	if s3ForcePathStyle {
		cfg = cfg.WithS3ForcePathStyle(true)
	}
	cfg = cfg.WithMaxRetries(c.values.Blockstore.S3.MaxRetries)
	return cfg
}

func (c *Config) GetBlockstoreType() string {
	return c.values.Blockstore.Type
}

func (c *Config) GetBlockstoreDefaultNamespacePrefix() string {
	return c.values.Blockstore.DefaultNamespacePrefix
}

func (c *Config) GetBlockAdapterS3Params() (blockparams.S3, error) {
	cfg := c.GetAwsConfig()

	return blockparams.S3{
		AwsConfig:                     cfg,
		StreamingChunkSize:            c.values.Blockstore.S3.StreamingChunkSize,
		StreamingChunkTimeout:         c.values.Blockstore.S3.StreamingChunkTimeout,
		DiscoverBucketRegion:          c.values.Blockstore.S3.DiscoverBucketRegion,
		SkipVerifyCertificateTestOnly: c.values.Blockstore.S3.SkipVerifyCertificateTestOnly,
		ServerSideEncryption:          c.values.Blockstore.S3.ServerSideEncryption,
		ServerSideEncryptionKmsKeyID:  c.values.Blockstore.S3.ServerSideEncryptionKmsKeyID,
	}, nil
}

func (c *Config) GetBlockAdapterLocalParams() (blockparams.Local, error) {
	localPath := c.values.Blockstore.Local.Path
	path, err := homedir.Expand(localPath)
	if err != nil {
		return blockparams.Local{}, fmt.Errorf("parse blockstore location URI %s: %w", localPath, err)
	}

	return blockparams.Local{Path: path}, nil
}

func (c *Config) GetBlockAdapterGSParams() (blockparams.GS, error) {
	return blockparams.GS{
		CredentialsFile: c.values.Blockstore.GS.CredentialsFile,
		CredentialsJSON: c.values.Blockstore.GS.CredentialsJSON,
	}, nil
}

func (c *Config) GetBlockAdapterAzureParams() (blockparams.Azure, error) {
	return blockparams.Azure{
		StorageAccount:   c.values.Blockstore.Azure.StorageAccount,
		StorageAccessKey: c.values.Blockstore.Azure.StorageAccessKey,
		AuthMethod:       c.values.Blockstore.Azure.AuthMethod,
		TryTimeout:       c.values.Blockstore.Azure.TryTimeout,
	}, nil
}

func (c *Config) GetAuthCacheConfig() authparams.ServiceCache {
	return authparams.ServiceCache{
		Enabled:        c.values.Auth.Cache.Enabled,
		Size:           c.values.Auth.Cache.Size,
		TTL:            c.values.Auth.Cache.TTL,
		EvictionJitter: c.values.Auth.Cache.Jitter,
	}
}

func (c *Config) GetAuthEncryptionSecret() []byte {
	secret := c.values.Auth.Encrypt.SecretKey
	if len(secret) == 0 {
		panic(fmt.Errorf("%w. Please set it to a unique, randomly generated value and store it somewhere safe", ErrMissingSecretKey))
	}
	return []byte(secret)
}

func (c *Config) GetS3GatewayRegion() string {
	return c.values.Gateways.S3.Region
}

func (c *Config) GetS3GatewayDomainNames() []string {
	return c.values.Gateways.S3.DomainNames
}

func (c *Config) GetS3GatewayFallbackURL() string {
	return c.values.Gateways.S3.FallbackURL
}

func (c *Config) GetListenAddress() string {
	return c.values.ListenAddress
}

func (c *Config) GetActionsEnabled() bool {
	return c.values.Actions.Enabled
}

func (c *Config) GetStatsEnabled() bool {
	return c.values.Stats.Enabled
}

func (c *Config) GetStatsAddress() string {
	return c.values.Stats.Address
}

func (c *Config) GetStatsFlushInterval() time.Duration {
	return c.values.Stats.FlushInterval
}

func (c *Config) GetStatsFlushSize() int {
	return c.values.Stats.FlushSize
}

func (c *Config) GetStatsExtended() bool {
	return c.values.Stats.Extended
}

func (c *Config) IsEmailSubscriptionEnabled() bool {
	return c.values.EmailSubscription.Enabled
}

func (c *Config) GetEmailParams() (email.Params, error) {
	return email.Params{
		SMTPHost:           c.values.Email.SMTPHost,
		SMTPPort:           c.values.Email.SMTPPort,
		UseSSL:             c.values.Email.UseSSL,
		Username:           c.values.Email.Username,
		Password:           c.values.Email.Password,
		LocalName:          c.values.Email.LocalName,
		Sender:             c.values.Email.Sender,
		LimitEveryDuration: c.values.Email.LimitEveryDuration,
		Burst:              c.values.Email.Burst,
		LakefsBaseURL:      c.values.Email.LakefsBaseURL,
	}, nil
}

const floatSumTolerance = 1e-6

// GetCommittedTierFSParams returns parameters for building a tierFS.  Caller must separately
// build and populate Adapter.
func (c *Config) GetCommittedTierFSParams(adapter block.Adapter) (*pyramidparams.ExtParams, error) {
	rangePro := c.values.Committed.LocalCache.RangeProportion
	metaRangePro := c.values.Committed.LocalCache.MetaRangeProportion

	if math.Abs(rangePro+metaRangePro-1) > floatSumTolerance {
		return nil, fmt.Errorf("range_proportion(%f) and metarange_proportion(%f): %w", rangePro, metaRangePro, ErrInvalidProportion)
	}

	localCacheDir, err := homedir.Expand(c.values.Committed.LocalCache.Dir)
	if err != nil {
		return nil, fmt.Errorf("expand %s: %w", c.values.Committed.LocalCache.Dir, err)
	}

	logger := logging.Default().WithField("module", "pyramid")
	return &pyramidparams.ExtParams{
		RangeAllocationProportion:     rangePro,
		MetaRangeAllocationProportion: metaRangePro,
		SharedParams: pyramidparams.SharedParams{
			Logger:             logger,
			Adapter:            adapter,
			BlockStoragePrefix: c.values.Committed.BlockStoragePrefix,
			Local: pyramidparams.LocalDiskParams{
				BaseDir:             localCacheDir,
				TotalAllocatedBytes: c.values.Committed.LocalCache.SizeBytes,
			},
			PebbleSSTableCacheSizeBytes: c.values.Committed.SSTable.Memory.CacheSizeBytes,
		},
	}, nil
}

func (c *Config) GetCommittedParams() *committed.Params {
	return &committed.Params{
		MinRangeSizeBytes:          c.values.Committed.Permanent.MinRangeSizeBytes,
		MaxRangeSizeBytes:          c.values.Committed.Permanent.MaxRangeSizeBytes,
		RangeSizeEntriesRaggedness: c.values.Committed.Permanent.RangeRaggednessEntries,
		MaxUploaders:               c.values.Committed.LocalCache.MaxUploadersPerWriter,
	}
}

func (c *Config) GetFixedInstallationID() string {
	return c.values.Installation.FixedID
}

func (c *Config) GetCommittedBlockStoragePrefix() string {
	return c.values.Committed.BlockStoragePrefix
}

func (c *Config) ToLoggerFields() logging.Fields {
	return MapLoggingFields(c.values)
}

func (c *Config) GetLoggingTraceRequestHeaders() bool {
	return c.values.Logging.TraceRequestHeaders
}

func (c *Config) GetAuditLogLevel() string {
	return c.values.Logging.AuditLogLevel
}

func (c *Config) GetSecurityAuditCheckInterval() time.Duration {
	return c.values.Security.AuditCheckInterval
}

func (c *Config) GetSecurityAuditCheckURL() string {
	return c.values.Security.AuditCheckURL
}

func (c *Config) GetAuthAPIEndpoint() string {
	return c.values.Auth.API.Endpoint
}

func (c *Config) IsAuthTypeAPI() bool {
	return c.values.Auth.API.Endpoint != ""
}

func (c *Config) GetAuthAPIToken() string {
	return c.values.Auth.API.Token
}

func (c *Config) GetAuthAPISupportsInvites() bool {
	return c.values.Auth.API.SupportsInvites
}

func (c *Config) GetUISnippets() []apiparams.CodeSnippet {
	snippets := make([]apiparams.CodeSnippet, 0, len(c.values.UI.Snippets))
	for _, item := range c.values.UI.Snippets {
		snippets = append(snippets, apiparams.CodeSnippet{
			ID:   item.ID,
			Code: item.Code,
		})
	}
	return snippets
}

func (c *Config) GetAuthOIDCConfiguration() OIDC {
	return c.values.Auth.OIDC
}

func (c *Config) GetAuthLogoutRedirectURL() string {
	return c.values.Auth.LogoutRedirectURL
}

func (c *Config) GetUIEnabled() bool {
	return c.values.UI.Enabled
}

func (c *Config) GetLoginDuration() time.Duration {
	return c.values.Auth.LoginDuration
}

func (c *Config) GravelerRepositoryCacheConfig() ref.CacheConfig {
	return ref.CacheConfig{
		Size:   c.values.Graveler.RepositoryCache.Size,
		Expiry: c.values.Graveler.RepositoryCache.Expiry,
		Jitter: c.values.Graveler.RepositoryCache.Jitter,
	}
}

func (c *Config) GravelerCommitCacheConfig() ref.CacheConfig {
	return ref.CacheConfig{
		Size:   c.values.Graveler.CommitCache.Size,
		Expiry: c.values.Graveler.CommitCache.Expiry,
		Jitter: c.values.Graveler.CommitCache.Jitter,
	}
}
