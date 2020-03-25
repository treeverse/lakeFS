package config

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"

	"github.com/treeverse/lakefs/logging"

	"github.com/aws/aws-sdk-go/aws/credentials"

	"github.com/aws/aws-sdk-go/aws"

	"github.com/aws/aws-sdk-go/service/s3"

	"github.com/aws/aws-sdk-go/aws/session"

	"github.com/treeverse/lakefs/block"

	"github.com/mitchellh/go-homedir"

	"github.com/dgraph-io/badger"
	"github.com/treeverse/lakefs/db"

	log "github.com/sirupsen/logrus"

	"github.com/spf13/viper"
)

const (
	ModuleName           = "github.com/treeverse/lakefs"
	ProjectDirectoryName = "lakefs"

	DefaultLoggingFormat = "text"
	DefaultLoggingLevel  = "INFO"
	DefaultLoggingOutput = "-"

	DefaultMetadataDBType         = "badger"
	DefaultMetadataBadgerPath     = "~/lakefs/metadata"
	DefaultMetadataBadgerTruncate = true

	DefaultBlockStoreType      = "local"
	DefaultBlockStoreLocalPath = "~/lakefs/data"
	DefaultBlockStoreS3Region  = "us-east-1"

	DefaultS3GatewayListenAddr = "0.0.0.0:8000"
	DefaultS3GatewayDomainName = "s3.local"
	DefaultS3GatewayRegion     = "us-east-1"

	DefaultAPIListenAddr = "0.0.0.0:8001"
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

	viper.SetDefault("metadata.db.type", DefaultMetadataDBType)
	viper.SetDefault("metadata.db.badger.path", DefaultMetadataBadgerPath)
	viper.SetDefault("metadata.db.badger.truncate", DefaultMetadataBadgerTruncate)

	viper.SetDefault("blockstore.type", DefaultBlockStoreType)
	viper.SetDefault("blockstore.local.path", DefaultBlockStoreLocalPath)
	viper.SetDefault("blockstore.s3.region", DefaultBlockStoreS3Region)

	viper.SetDefault("gateways.s3.listen_address", DefaultS3GatewayListenAddr)
	viper.SetDefault("gateways.s3.domain_name", DefaultS3GatewayDomainName)
	viper.SetDefault("gateways.s3.region", DefaultS3GatewayRegion)

	viper.SetDefault("api.listen_address", DefaultAPIListenAddr)
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

func (c *Config) BuildStore() db.Store {

	if !strings.EqualFold(viper.GetString("metadata.db.type"), "badger") {
		panic(fmt.Errorf("metadata db: only badgerDB is currently supported"))
	}

	location := viper.GetString("metadata.db.badger.path")
	location, err := homedir.Expand(location)
	if err != nil {
		panic(fmt.Errorf("could not parse metadata location URI: %s\n", err))
	}

	opts := badger.DefaultOptions(location).
		WithTruncate(viper.GetBool("metadata.db.badger.truncate")).
		WithLogger(logging.Default().WithField("subsystem", "badger"))
	bdb, err := badger.Open(opts)
	if err != nil {
		panic(fmt.Errorf("could not open badgerDB database: %s\n", err))
	}
	log.WithFields(log.Fields{
		"type":     "badger",
		"location": location,
		"truncate": viper.GetBool("metadata.db.badger.truncate"),
	}).Info("initialize metadata store")
	return db.NewLocalDBStore(bdb)
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
	svc := s3.New(sess)
	adapter, err := block.NewS3Adapter(svc)
	if err != nil {
		panic(fmt.Errorf("got error opening an S3 block adapter: %s", err))
	}
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
