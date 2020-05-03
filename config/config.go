package config

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/treeverse/lakefs/block"
	s3a "github.com/treeverse/lakefs/block/s3"
	"github.com/treeverse/lakefs/db"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/mitchellh/go-homedir"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

const (
	ModuleName           = "github.com/treeverse/lakefs"
	ProjectDirectoryName = "lakefs"

	DefaultLoggingFormat = "text"
	DefaultLoggingLevel  = "INFO"
	DefaultLoggingOutput = "-"

	DefaultDatabaseDriver = "pgx"
	DefaultMetadataDBUri  = "postgres://localhost:5432/postgres?search_path=lakefs_index"
	DefaultAuthDBUri      = "postgres://localhost:5432/postgres?search_path=lakefs_auth"

	DefaultBlockStoreType      = "local"
	DefaultBlockStoreLocalPath = "~/lakefs/data"
	DefaultBlockStoreS3Region  = "us-east-1"

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

type Config struct {
}

func (c *Config) setDefaults() {
	viper.SetDefault("logging.format", DefaultLoggingFormat)
	viper.SetDefault("logging.level", DefaultLoggingLevel)
	viper.SetDefault("logging.output", DefaultLoggingOutput)

	viper.SetDefault("metadata.db.uri", DefaultMetadataDBUri)

	viper.SetDefault("auth.db.uri", DefaultAuthDBUri)

	viper.SetDefault("blockstore.type", DefaultBlockStoreType)
	viper.SetDefault("blockstore.local.path", DefaultBlockStoreLocalPath)
	viper.SetDefault("blockstore.s3.region", DefaultBlockStoreS3Region)

	viper.SetDefault("gateways.s3.listen_address", DefaultS3GatewayListenAddr)
	viper.SetDefault("gateways.s3.domain_name", DefaultS3GatewayDomainName)
	viper.SetDefault("gateways.s3.region", DefaultS3GatewayRegion)

	viper.SetDefault("api.listen_address", DefaultAPIListenAddr)

	viper.SetDefault("stats.enabled", DefaultStatsEnabled)
	viper.SetDefault("stats.address", DefaultStatsAddr)
	viper.SetDefault("stats.flush_interval", DefaultStatsFlushInterval)
}

func NewFromFile(configPath string) *Config {
	handle, err := os.Open(configPath)
	if err != nil {
		panic(fmt.Errorf("failed opening config file %s: %s", configPath, err))
	}
	c := &Config{}
	c.Setup(handle)
	return c
}

func New() *Config {
	c := &Config{}
	c.Setup(nil)
	return c
}

func (c *Config) Setup(confReader io.Reader) {
	viper.SetConfigType("yaml")
	viper.SetEnvPrefix("LAKEFS")
	viper.AutomaticEnv()

	c.setDefaults()

	if confReader != nil {
		err := viper.ReadConfig(confReader)
		if err != nil {
			panic(fmt.Errorf("Error reading config file: %s\n", err))
		}
	}

	c.setupLogger()
}

func (c *Config) setupLogger() {
	// add calling function/line numbers to log lines
	log.SetReportCaller(true)

	// trim calling function so it looks more readable
	logPrettyfier := func(frame *runtime.Frame) (function string, file string) {
		indexOfModule := strings.Index(strings.ToLower(frame.File), ProjectDirectoryName)
		if indexOfModule != -1 {
			file = frame.File[indexOfModule+len(ProjectDirectoryName):]
		} else {
			file = frame.File
		}
		file = fmt.Sprintf("%s:%d", strings.TrimPrefix(file, string(os.PathSeparator)), frame.Line)
		function = strings.TrimPrefix(frame.Function, fmt.Sprintf("%s%s", ModuleName, string(os.PathSeparator)))
		return
	}

	// set output format
	if strings.EqualFold(viper.GetString("logging.format"), "text") {
		log.SetFormatter(&log.TextFormatter{
			ForceColors:      true,
			FullTimestamp:    true,
			CallerPrettyfier: logPrettyfier,
		})
	} else if strings.EqualFold(viper.GetString("logging.format"), "json") {
		log.SetFormatter(&log.JSONFormatter{
			CallerPrettyfier: logPrettyfier,
			PrettyPrint:      false,
		})
	}

	// set output
	if strings.EqualFold(viper.GetString("logging.output"), "-") {
		log.SetOutput(os.Stdout)
	} else {
		filename := viper.GetString("logging.output")
		handle, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0755)
		if err != nil {
			panic(fmt.Errorf("could not open log file: %s", err))
		}
		log.SetOutput(handle)
		// setup signal handler to reopen logfile on SIGHUP
		sigChannel := make(chan os.Signal, 1)
		signal.Notify(sigChannel, syscall.SIGHUP)
		go func() {
			for {
				<-sigChannel
				log.Info("SIGHUP received, rotating log file")
				log.SetOutput(ioutil.Discard)
				_ = handle.Close()
				handle, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0755)
				if err != nil {
					panic(fmt.Errorf("could not open log file: %s", err))
				}
				log.SetOutput(handle)
				log.Info("log file was rotated successfully")
			}
		}()
	}

	// set level
	switch strings.ToLower(viper.GetString("logging.level")) {
	case "trace":
		log.SetLevel(log.TraceLevel)
	case "debug":
		log.SetLevel(log.DebugLevel)
	case "info":
		log.SetLevel(log.InfoLevel)
	case "warn", "warning":
		log.SetLevel(log.WarnLevel)
	case "error":
		log.SetLevel(log.ErrorLevel)
	case "null", "none":
		log.SetOutput(ioutil.Discard)
	}
}

func getSearchPath(uri string) (string, error) {
	parsed, err := url.Parse(uri)
	if err != nil {
		return "", err
	}
	sp := parsed.Query().Get("search_path")
	if len(sp) > 0 {
		return sp, nil
	}
	return "", fmt.Errorf("search_path not present in database connection string")
}

func setupDb(uri string) db.Database {
	schema, err := getSearchPath(uri)
	if err != nil {
		panic(fmt.Errorf("could not open database: %s\n", err))
	}
	conn, err := sqlx.Connect(DefaultDatabaseDriver, uri)
	if err != nil {
		panic(fmt.Errorf("could not open database: %s\n", err))
	}
	tx, err := conn.Beginx()
	if err != nil {
		panic(fmt.Errorf("could not open database: %s\n", err))
	}
	_, err = tx.Exec(fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS  %s`, schema))
	if err != nil {
		panic(fmt.Errorf("could not open database: %s\n", err))
	}
	err = tx.Commit()
	if err != nil {
		panic(fmt.Errorf("could not open database: %s\n", err))
	}
	return db.NewDatabase(conn)
}

func (c *Config) BuildMetadataDatabase() db.Database {
	return setupDb(viper.GetString("metadata.db.uri"))

}

func (c *Config) BuildAuthDatabase() db.Database {
	return setupDb(viper.GetString("auth.db.uri"))
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
	adapter := s3a.NewAdapter(svc)
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

	adapter, err := block.NewLocalFSAdapter(location)
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
	default:
		panic(fmt.Errorf("%s is not a valid blockstore type, please choose one of \"s3\" or \"local\"",
			viper.GetString("blockstore.type")))
	}
}

func (c *Config) GetAuthEncryptionSecret() string {
	secret := viper.GetString("auth.encrypt.secret_key")
	if len(secret) == 0 {
		panic(fmt.Errorf("auth.encrypt.secret_key cannot be empty. Please set it to a unique, randomly generated value and store it somewhere safe"))
	}
	return secret
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
