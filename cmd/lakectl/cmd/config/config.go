package config

import (
	"errors"
	"reflect"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/spf13/viper"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/logging"
)

// configuration is the user-visible configuration structure in Golang form.  When editing
// make sure *all* fields have a `mapstructure:"..."` tag, to simplify future refactoring.
type configuration struct {
	Credentials struct {
		AccessKeyID     string `mapstructure:"access_key_id"`
		SecretAccessKey string `mapstructure:"secret_access_key"`
	}
	Server struct {
		EndpointURL string `mapstructure:"endpoint_url"`
	}
	Metastore struct {
		Type string `mapstructure:"type"`
		Hive struct {
			URI           string `mapstructure:"uri"`
			DBLocationURI string `mapstructure:"db_location_uri"`
		} `mapstructure:"hive"`
		Glue struct {
			// TODO(ariels): Refactor credentials to share with server side.
			Profile         string `mapstructure:"profile"`
			CredentialsFile string `mapstructure:"credentials_file"`
			DBLocationURI   string `mapstructure:"db_location_uri"`
			Credentials     *struct {
				AccessKeyID     string `mapstructure:"access_key_id"`
				AccessSecretKey string `mapstructure:"access_secret_key"`
				SessionToken    string `mapstructure:"session_token"`
			} `mapstructure:"credentials"`

			Region    string            `mapstructure:"region"`
			CatalogID config.OnlyString `mapstructure:"catalog_id"`
		}
		// setting FixSparkPlaceholder to true will change spark placeholder with the actual location. for more information see https://github.com/treeverse/lakeFS/issues/2213
		FixSparkPlaceholder bool `mapstructure:"fix_spark_placeholder"`
	}
}

// Config is the currently-loaded configuration.  Its error state supports being able to run
// 'lakectl config' without a valid configuration.
type Config struct {
	Values configuration
	err    error
}

// ReadConfig loads according to the current viper configuration into a Config, which will
// have non-nil Err() if loading fails.
func ReadConfig() (c *Config) {
	c = &Config{}

	// Inform viper of all expected fields.  Otherwise it fails to deserialize from the
	// environment.
	keys := config.GetStructKeys(reflect.TypeOf(c.Values), "mapstructure", "squash")
	for _, key := range keys {
		viper.SetDefault(key, nil)
	}

	setDefaults()
	c.err = viper.ReadInConfig()
	logger := logging.Default().WithField("file", viper.ConfigFileUsed())
	if errors.Is(c.err, viper.ConfigFileNotFoundError{}) {
		logger.WithError(c.err).Fatal("failed to read config file")
	}
	if c.err != nil {
		logger.WithError(c.err).Fatal("failed to read config file")
	}
	return
}

const (
	// Default flag keys
	HiveDBLocationURIKey       = "metastore.hive.db_location_uri"
	ConfigServerEndpointURLKey = "server.endpoint_url"
	ConfigAccessKeyIDKey       = "credentials.access_key_id"
	ConfigSecretAccessKey      = "credentials.secret_access_key"

	// Defaults
	DefaultHiveDBLocationURI = "file:/user/hive/warehouse/"
	DefaultServerEndpointURL = "http://127.0.0.1:8000"
)

func setDefaults() {
	viper.SetDefault(HiveDBLocationURIKey, DefaultHiveDBLocationURI)
	viper.SetDefault(ConfigServerEndpointURLKey, DefaultServerEndpointURL)
}

func (c *Config) Err() error {
	return c.err
}

func (c *Config) GetMetastoreAwsConfig() *aws.Config {
	cfg := &aws.Config{
		Region: aws.String(c.Values.Metastore.Glue.Region),
	}
	if c.Values.Metastore.Glue.Profile != "" || c.Values.Metastore.Glue.CredentialsFile != "" {
		cfg.Credentials = credentials.NewSharedCredentials(
			c.Values.Metastore.Glue.CredentialsFile,
			c.Values.Metastore.Glue.Profile,
		)
	}
	if c.Values.Metastore.Glue.Credentials != nil {
		cfg.Credentials = credentials.NewStaticCredentials(
			c.Values.Metastore.Glue.Credentials.AccessKeyID,
			c.Values.Metastore.Glue.Credentials.AccessSecretKey,
			c.Values.Metastore.Glue.Credentials.SessionToken,
		)
	}
	return cfg
}

func (c *Config) GetMetastoreHiveURI() string {
	return c.Values.Metastore.Hive.URI
}

func (c *Config) GetMetastoreGlueCatalogID() string {
	return string(c.Values.Metastore.Glue.CatalogID)
}

func (c *Config) GetMetastoreType() string {
	return c.Values.Metastore.Type
}

func (c *Config) GetHiveDBLocationURI() string {
	return c.Values.Metastore.Hive.DBLocationURI
}

func (c *Config) GetGlueDBLocationURI() string {
	return c.Values.Metastore.Glue.DBLocationURI
}

func (c *Config) GetFixSparkPlaceholder() bool {
	return c.Values.Metastore.FixSparkPlaceholder
}
