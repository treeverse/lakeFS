package config

import (
	"fmt"
	"time"

	"github.com/treeverse/lakefs/logging"

	"github.com/treeverse/lakefs/block/local"
	"github.com/treeverse/lakefs/block/mem"

	"github.com/google/uuid"
	"github.com/treeverse/lakefs/stats"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/mitchellh/go-homedir"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/treeverse/lakefs/block"
	s3a "github.com/treeverse/lakefs/block/s3"
	"github.com/treeverse/lakefs/db"
)

const (
	DefaultDatabaseDriver = "pgx"
	DefaultCatalogDBUri   = "postgres://localhost:5432/postgres?search_path=lakefs_catalog&sslmode=disable"
	DefaultMetadataDBUri  = "postgres://localhost:5432/postgres?search_path=lakefs_index&sslmode=disable"
	DefaultAuthDBUri      = "postgres://localhost:5432/postgres?search_path=lakefs_auth&sslmode=disable"

	DefaultBlockStoreType                    = "local"
	DefaultBlockStoreLocalPath               = "~/lakefs/data"
	DefaultBlockStoreS3Region                = "us-east-1"
	DefaultBlockStoreS3StreamingChunkSize    = 2 << 19         // 1MiB by default per chunk
	DefaultBlockStoreS3StreamingChunkTimeout = time.Second * 1 // or 1 seconds, whatever comes first

	DefaultS3GatewayListenAddr = "0.0.0.0:8000"
	DefaultS3GatewayDomainName = "s3.local.lakefs.io"
	DefaultS3GatewayRegion     = "us-east-1"

	DefaultAPIListenAddr = "0.0.0.0:8001"

	DefaultStatsEnabled       = true
	DefaultStatsAddr          = "https://stats.treeverse.io"
	DefaultStatsFlushInterval = time.Second * 30
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
	viper.SetDefault("logging.format", DefaultLoggingFormat)
	viper.SetDefault("logging.level", DefaultLoggingLevel)
	viper.SetDefault("logging.output", DefaultLoggingOutput)

	viper.SetDefault("catalog.db.uri", DefaultCatalogDBUri)
	viper.SetDefault("metadata.db.uri", DefaultMetadataDBUri)

	viper.SetDefault("auth.db.uri", DefaultAuthDBUri)

	viper.SetDefault("blockstore.type", DefaultBlockStoreType)
	viper.SetDefault("blockstore.local.path", DefaultBlockStoreLocalPath)
	viper.SetDefault("blockstore.s3.region", DefaultBlockStoreS3Region)
	viper.SetDefault("blockstore.s3.streaming_chunk_size", DefaultBlockStoreS3StreamingChunkSize)
	viper.SetDefault("blockstore.s3.streaming_chunk_timeout", DefaultBlockStoreS3StreamingChunkTimeout)

	viper.SetDefault("gateways.s3.listen_address", DefaultS3GatewayListenAddr)
	viper.SetDefault("gateways.s3.domain_name", DefaultS3GatewayDomainName)
	viper.SetDefault("gateways.s3.region", DefaultS3GatewayRegion)

	viper.SetDefault("api.listen_address", DefaultAPIListenAddr)

	viper.SetDefault("stats.enabled", DefaultStatsEnabled)
	viper.SetDefault("stats.address", DefaultStatsAddr)
	viper.SetDefault("stats.flush_interval", DefaultStatsFlushInterval)
}

const (
	DBKeyAuth    = "auth"
	DBKeyIndex   = "metadata"
	DBKeyCatalog = "catalog"
)

var SchemaDBKeys = map[string]string{
	SchemaAuth:     DBKeyAuth,
	SchemaMetadata: DBKeyIndex,
	SchemaCatalog:  DBKeyCatalog,
}

func (c *Config) GetDatabaseURI(key string) string {
	return viper.GetString(key + ".db.uri")
}

func (c *Config) ConnectMetadataDatabase() db.Database {
	database, err := db.ConnectDB(DefaultDatabaseDriver, c.GetDatabaseURI(DBKeyIndex))
	if err != nil {
		panic(err)
	}
	return database
}

func (c *Config) ConnectAuthDatabase() db.Database {
	database, err := db.ConnectDB(DefaultDatabaseDriver, c.GetDatabaseURI(DBKeyAuth))
	if err != nil {
		panic(err)
	}
	return database
}

func (c *Config) buildS3Adapter() block.Adapter {
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

	sess := session.Must(session.NewSession(cfg))
	sess.ClientConfig(s3.ServiceName)
	svc := s3.New(sess)
	adapter := s3a.NewAdapter(svc,
		s3a.WithStreamingChunkSize(viper.GetInt("blockstore.s3.streaming_chunk_size")),
		s3a.WithStreamingChunkTimeout(viper.GetDuration("blockstore.s3.streaming_chunk_timeout")))
	log.WithFields(log.Fields{
		"type": "s3",
	}).Info("initialized blockstore adapter")
	return adapter
}

func (c *Config) buildLocalAdapter() block.Adapter {
	location := viper.GetString("blockstore.local.path")
	location, err := homedir.Expand(location)
	if err != nil {
		panic(fmt.Errorf("could not parse metadata location URI: %s\n", err))
	}

	adapter, err := local.NewAdapter(location)
	if err != nil {
		panic(fmt.Errorf("got error opening a local block adapter with path %s: %s", location, err))
	}
	log.WithFields(log.Fields{
		"type": "local",
		"path": location,
	}).Info("initialized blockstore adapter")
	return adapter
}

func (c *Config) BuildBlockAdapter() block.Adapter {
	switch viper.GetString("blockstore.type") {
	case "local":
		return c.buildLocalAdapter()
	case "s3":
		return c.buildS3Adapter()
	case "mem", "memory":
		logging.Default().
			WithField("type", "mem").
			Info("initialized blockstore adapter")
		return mem.New()
	default:
		panic(fmt.Errorf("%s is not a valid blockstore type, please choose one of \"s3\", \"local\" or \"mem\"",
			viper.GetString("blockstore.type")))
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

func (c *Config) GetS3GatewayListenAddress() string {
	return viper.GetString("gateways.s3.listen_address")
}

func (c *Config) GetS3GatewayDomainName() string {
	return viper.GetString("gateways.s3.domain_name")
}

func (c *Config) GetAPIListenAddress() string {
	return viper.GetString("api.listen_address")
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
		sender = stats.NewHTTPSender(installationID, uuid.New().String(), c.GetStatsAddress(), time.Now)
	}
	return stats.NewBufferedCollector(
		stats.WithSender(sender),
		stats.WithFlushInterval(c.GetStatsFlushInterval()))
}
