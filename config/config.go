package config

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
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

func (c *Config) buildS3Adapter() (block.Adapter, error) {
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
	default:
		return nil, fmt.Errorf("blockstore '%s' is not a valid type, please choose one of %s",
			blockstore, []string{local.BlockstoreType, s3a.BlockstoreType, mem.BlockstoreType, transient.BlockstoreType})
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
		panic(fmt.Errorf("auth.encrypt.secret_key cannot be empty. Please set it to a unique, randomly generated value and store it somewhere safe"))
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
