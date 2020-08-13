package config

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/treeverse/lakefs/block/gcs"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/sts"
	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/mitchellh/go-homedir"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/treeverse/lakefs/auth"
	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/block/local"
	"github.com/treeverse/lakefs/block/mem"
	s3a "github.com/treeverse/lakefs/block/s3"
	"github.com/treeverse/lakefs/block/transient"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/logging"
	"github.com/treeverse/lakefs/stats"
)

const (
	DefaultDatabaseDriver     = "pgx"
	DefaultDatabaseConnString = "postgres://localhost:5432/postgres?sslmode=disable"

	DefaultBlockStoreType                    = "local"
	DefaultBlockStoreLocalPath               = "~/lakefs/data"
	DefaultBlockStoreS3Region                = "us-east-1"
	DefaultBlockStoreS3StreamingChunkSize    = 2 << 19         // 1MiB by default per chunk
	DefaultBlockStoreS3StreamingChunkTimeout = time.Second * 1 // or 1 seconds, whatever comes first

	DefaultBlockStoreGCSS3Endpoint            = "https://storage.googleapis.com"
	DefaultBlockStoreGCSStreamingChunkSize    = 2 << 19         // 1MiB by default per chunk
	DefaultBlockStoreGCSStreamingChunkTimeout = time.Second * 1 // or 1 seconds, whatever comes first

	DefaultAuthCacheEnabled = true
	DefaultAuthCacheSize    = 1024
	DefaultAuthCacheTTL     = 20 * time.Second
	DefaultAuthCacheJitter  = 3 * time.Second

	DefaultListenAddr          = "0.0.0.0:8000"
	DefaultS3GatewayDomainName = "s3.local.lakefs.io"
	DefaultS3GatewayRegion     = "us-east-1"

	DefaultStatsEnabled       = true
	DefaultStatsAddr          = "https://stats.treeverse.io"
	DefaultStatsFlushInterval = time.Second * 30

	MetaStoreType          = "metastore.type"
	MetaStoreHiveURI       = "metastore.hive.uri"
	MetastoreGlueCatalogID = "metastore.glue.catalog-id"
)

var (
	ErrInvalidBlockStoreType = errors.New("invalid blockstore type")
	ErrMissingSecretKey      = errors.New("auth.encrypt.secret_key cannot be empty")
)

type LogrusAWSAdapter struct {
	logger *log.Entry
}

func (l *LogrusAWSAdapter) Log(vars ...interface{}) {
	l.logger.Debug(vars...)
}

type Config struct{}

func NewConfig() *Config {
	setDefaults()
	setupLogger()
	return &Config{}
}

func setDefaults() {
	viper.SetDefault("listen_address", DefaultListenAddr)

	viper.SetDefault("logging.format", DefaultLoggingFormat)
	viper.SetDefault("logging.level", DefaultLoggingLevel)
	viper.SetDefault("logging.output", DefaultLoggingOutput)

	viper.SetDefault("database.connection_string", DefaultDatabaseConnString)

	viper.SetDefault("auth.cache.enabled", DefaultAuthCacheEnabled)
	viper.SetDefault("auth.cache.size", DefaultAuthCacheSize)
	viper.SetDefault("auth.cache.ttl", DefaultAuthCacheTTL)
	viper.SetDefault("auth.cache.jitter", DefaultAuthCacheJitter)

	viper.SetDefault("blockstore.type", DefaultBlockStoreType)
	viper.SetDefault("blockstore.local.path", DefaultBlockStoreLocalPath)
	viper.SetDefault("blockstore.s3.region", DefaultBlockStoreS3Region)
	viper.SetDefault("blockstore.s3.streaming_chunk_size", DefaultBlockStoreS3StreamingChunkSize)
	viper.SetDefault("blockstore.s3.streaming_chunk_timeout", DefaultBlockStoreS3StreamingChunkTimeout)

	viper.SetDefault("gateways.s3.domain_name", DefaultS3GatewayDomainName)
	viper.SetDefault("gateways.s3.region", DefaultS3GatewayRegion)

	viper.SetDefault("blockstore.gcs.s3_endpoint", DefaultBlockStoreGCSS3Endpoint)
	viper.SetDefault("blockstore.gcs.streaming_chunk_size", DefaultBlockStoreGCSStreamingChunkSize)
	viper.SetDefault("blockstore.gcs.streaming_chunk_timeout", DefaultBlockStoreGCSStreamingChunkTimeout)

	viper.SetDefault("stats.enabled", DefaultStatsEnabled)
	viper.SetDefault("stats.address", DefaultStatsAddr)
	viper.SetDefault("stats.flush_interval", DefaultStatsFlushInterval)
}

func (c *Config) GetDatabaseURI() string {
	return viper.GetString("database.connection_string")
}

func (c *Config) BuildDatabaseConnection() db.Database {
	database, err := db.ConnectDB(DefaultDatabaseDriver, c.GetDatabaseURI())
	if err != nil {
		panic(err)
	}
	return database
}

type AwsS3RetentionConfig struct {
	RoleArn           string
	ManifestBaseURL   *url.URL
	ReportS3PrefixURL *string
}

func (c *Config) GetAwsS3RetentionConfig() AwsS3RetentionConfig {
	var errs []string
	roleArn := viper.GetString("blockstore.s3.retention.role_arn")
	if roleArn == "" {
		errs = append(errs, "blockstore.s3.retention.role_arn")
	}

	manifestBaseURL, err := url.ParseRequestURI(viper.GetString("blockstore.s3.retention.manifest_base_url"))
	if err != nil {
		errs = append(errs, fmt.Sprintf("blockstore.s3.retention.manifest_base_url: %s", err))
	}
	if len(errs) > 0 {
		panic(fmt.Sprintf("need %s to handle retention on S3", strings.Join(errs, ", ")))
	}
	var reportS3PrefixURL *string
	prefixURL := viper.GetString("blockstore.s3.retention.report_s3_prefix_url")
	if prefixURL != "" {
		reportS3PrefixURL = &prefixURL
	}
	return AwsS3RetentionConfig{
		RoleArn:           roleArn,
		ManifestBaseURL:   manifestBaseURL,
		ReportS3PrefixURL: reportS3PrefixURL,
	}
}

func (c *Config) GetAwsConfig() *aws.Config {
	cfg := &aws.Config{
		Region: aws.String(viper.GetString("blockstore.s3.region")),
		Logger: &LogrusAWSAdapter{log.WithField("sdk", "aws")},
	}
	if viper.IsSet("blockstore.s3.profile") || viper.IsSet("blockstore.s3.credentials_file") {
		cfg.Credentials = credentials.NewSharedCredentials(
			viper.GetString("blockstore.s3.credentials_file"),
			viper.GetString("blockstore.s3.profile"))
	}
	if viper.IsSet("blockstore.s3.credentials") {
		cfg.Credentials = credentials.NewStaticCredentials(
			viper.GetString("blockstore.s3.credentials.access_key_id"),
			viper.GetString("blockstore.s3.credentials.access_secret_key"),
			viper.GetString("blockstore.s3.credentials.session_token"))
	}
	return cfg
}

func (c *Config) GetGCSAwsConfig() *aws.Config {
	cfg := &aws.Config{
		Region: aws.String(viper.GetString("blockstore.gcs.s3_region")),
		Logger: &LogrusAWSAdapter{log.WithField("sdk", "aws")},
	}
	if viper.IsSet("blockstore.gcs.s3_profile") || viper.IsSet("blockstore.gcs.s3_credentials_file") {
		cfg.Credentials = credentials.NewSharedCredentials(
			viper.GetString("blockstore.gcs.s3_credentials_file"),
			viper.GetString("blockstore.gcs.s3_profile"))
	}
	if viper.IsSet("blockstore.gcs.s3_credentials") {
		cfg.Credentials = credentials.NewStaticCredentials(
			viper.GetString("blockstore.gcs.s3_credentials.access_key_id"),
			viper.GetString("blockstore.gcs.s3_credentials.access_secret_key"),
			viper.GetString("blockstore.gcs.s3_credentials.session_token"))
	}
	return cfg
}

func GetAwsAccessKeyID(awsConfig *aws.Config) (string, error) {
	awsCredentials, err := awsConfig.Credentials.Get()
	if err != nil {
		return "", fmt.Errorf("access AWS credentials: %w", err)
	}
	return awsCredentials.AccessKeyID, nil
}

func GetAccount(awsConfig *aws.Config) (string, error) {
	accessKeyID, err := GetAwsAccessKeyID(awsConfig)
	if err != nil {
		return "", err
	}
	sess, err := session.NewSession(awsConfig)
	if err != nil {
		return "", fmt.Errorf("get AWS session: %w", err)
	}
	sess.ClientConfig(sts.ServiceName)
	svc := sts.New(sess)

	account, err := svc.GetAccessKeyInfo(&sts.GetAccessKeyInfoInput{
		AccessKeyId: aws.String(accessKeyID),
	})
	if err != nil {
		return "", fmt.Errorf("get access key info for %s: %w", accessKeyID, err)
	}
	return *account.Account, nil
}

func (c *Config) buildS3Adapter() (block.Adapter, error) {
	cfg := c.GetAwsConfig()

	sess, err := session.NewSession(cfg)
	if err != nil {
		return nil, err
	}
	sess.ClientConfig(s3.ServiceName)
	svc := s3.New(sess)
	adapter := s3a.NewAdapter(svc,
		s3a.WithStreamingChunkSize(viper.GetInt("blockstore.s3.streaming_chunk_size")),
		s3a.WithStreamingChunkTimeout(viper.GetDuration("blockstore.s3.streaming_chunk_timeout")))
	log.WithFields(log.Fields{
		"type": "s3",
	}).Info("initialized blockstore adapter")
	return adapter, nil
}

func (c *Config) buildGCSAdapter() (block.Adapter, error) {
	cfg := c.GetGCSAwsConfig()
	s3Endpoint := viper.GetString("blockstore.gcs.s3_endpoint")
	sess, err := session.NewSession(cfg)
	if err != nil {
		return nil, err
	}
	sess.ClientConfig(s3.ServiceName)
	svc := s3.New(sess, aws.NewConfig().WithEndpoint(s3Endpoint))
	adapter := gcs.NewAdapter(svc,
		gcs.WithStreamingChunkSize(viper.GetInt("blockstore.gcs.streaming_chunk_size")),
		gcs.WithStreamingChunkTimeout(viper.GetDuration("blockstore.gcs.streaming_chunk_timeout")))
	log.WithFields(log.Fields{"type": "gcs"}).Info("initialized blockstore adapter")
	return adapter, nil
}

func (c *Config) buildLocalAdapter() (block.Adapter, error) {
	localPath := viper.GetString("blockstore.local.path")
	location, err := homedir.Expand(localPath)
	if err != nil {
		return nil, fmt.Errorf("could not parse blockstore location URI: %w", err)
	}

	adapter, err := local.NewAdapter(location)
	if err != nil {
		return nil, fmt.Errorf("got error opening a local block adapter with path %s: %w", location, err)
	}
	log.WithFields(log.Fields{
		"type": "local",
		"path": location,
	}).Info("initialized blockstore adapter")
	return adapter, nil
}

func (c *Config) BuildBlockAdapter() (block.Adapter, error) {
	blockstore := viper.GetString("blockstore.type")
	logging.Default().
		WithField("type", blockstore).
		Info("initialize blockstore adapter")
	switch blockstore {
	case local.BlockstoreType:
		return c.buildLocalAdapter()
	case s3a.BlockstoreType:
		return c.buildS3Adapter()
	case mem.BlockstoreType, "memory":
		return mem.New(), nil
	case transient.BlockstoreType:
		return transient.New(), nil
	case gcs.BlockstoreType:
		return c.buildGCSAdapter()
	default:
		return nil, fmt.Errorf("%w '%s' please choose one of %s",
			ErrInvalidBlockStoreType, blockstore, []string{local.BlockstoreType, s3a.BlockstoreType, mem.BlockstoreType, transient.BlockstoreType, gcs.BlockstoreType})
	}
}

func (c *Config) GetAuthCacheConfig() auth.ServiceCacheConfig {
	return auth.ServiceCacheConfig{
		Enabled:        viper.GetBool("auth.cache.enabled"),
		Size:           viper.GetInt("auth.cache.size"),
		TTL:            viper.GetDuration("auth.cache.ttl"),
		EvictionJitter: viper.GetDuration("auth.cache.jitter"),
	}
}

func (c *Config) GetAuthEncryptionSecret() []byte {
	secret := viper.GetString("auth.encrypt.secret_key")
	if len(secret) == 0 {
		panic(fmt.Errorf("%w. Please set it to a unique, randomly generated value and store it somewhere safe", ErrMissingSecretKey))
	}
	return []byte(secret)
}

func (c *Config) GetS3GatewayRegion() string {
	return viper.GetString("gateways.s3.region")
}

func (c *Config) GetS3GatewayDomainName() string {
	return viper.GetString("gateways.s3.domain_name")
}

func (c *Config) GetListenAddress() string {
	return viper.GetString("listen_address")
}

func (c *Config) GetStatsEnabled() bool {
	return viper.GetBool("stats.enabled")
}

func (c *Config) GetStatsAddress() string {
	return viper.GetString("stats.address")
}

func (c *Config) GetStatsFlushInterval() time.Duration {
	return viper.GetDuration("stats.flush_interval")
}

func (c *Config) BuildStats(installationID string) *stats.BufferedCollector {
	sender := stats.NewDummySender()
	if c.GetStatsEnabled() && Version != UnreleasedVersion {
		sender = stats.NewHTTPSender(c.GetStatsAddress(), time.Now)
	}
	return stats.NewBufferedCollector(
		installationID,
		uuid.Must(uuid.NewUUID()).String(),
		stats.WithSender(sender),
		stats.WithFlushInterval(c.GetStatsFlushInterval()))
}

func GetMetastoreAwsConfig() *aws.Config {
	cfg := &aws.Config{
		Region: aws.String(viper.GetString("metastore.glue.region")),
		Logger: &LogrusAWSAdapter{},
	}
	if viper.IsSet("metastore.glue.profile") || viper.IsSet("metastore.glue.credentials_file") {
		cfg.Credentials = credentials.NewSharedCredentials(
			viper.GetString("metastore.glue.credentials_file"),
			viper.GetString("metastore.glue.profile"))
	}
	if viper.IsSet("metastore.glue.credentials") {
		cfg.Credentials = credentials.NewStaticCredentials(
			viper.GetString("metastore.glue.credentials.access_key_id"),
			viper.GetString("metastore.glue.credentials.access_secret_key"),
			viper.GetString("metastore.glue.credentials.session_token"))
	}
	return cfg
}

func GetMetastoreHiveURI() string {
	return viper.GetString(MetaStoreHiveURI)
}

func GetMetastoreGlueCatalogID() string {
	return viper.GetString(MetastoreGlueCatalogID)
}
func GetMetastoreType() string {
	return viper.GetString(MetaStoreType)
}
