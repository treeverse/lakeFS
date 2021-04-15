package config

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/spf13/viper"
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
			URI string `mapstructure:"uri"`
		} `mapstructure:"hive"`
		Glue struct {
			// TODO(ariels): Refactor credentials to share with server side.
			Profile         string `mapstructure:"profile"`
			CredentialsFile string `mapstructure:"credentials_file"`
			Credentials     *struct {
				AccessKeyID     string `mapstructure:"access_key_id"`
				AccessSecretKey string `mapstructure:"access_secret_key"`
				SessionToken    string `mapstructure:"session_token"`
			} `mapstructure:"credentials"`

			Region    string `mapstructure:"region"`
			CatalogID string `mapstructure:"catalog_id"`
		}
	}
}

// Config is the currently-loaded configuration.  Its error state supports being able to run
// `lakectl config' without a valid configuration.
type Config struct {
	configuration
	err error
}

// ReadConfig loads according to the current viper configuration into a Config, which will
// have non-nil Err() if loading fails.
func ReadConfig() (c *Config) {
	c = &Config{}
	c.err = viper.ReadInConfig()
	if c.err != nil {
		return
	}
	c.err = viper.UnmarshalExact(&c.configuration)
	return
}

func (c *Config) Err() error {
	return c.err
}

func (c *Config) GetMetastoreAwsConfig() *aws.Config {
	cfg := &aws.Config{
		Region: aws.String(c.configuration.Metastore.Glue.Region),
	}
	if c.Metastore.Glue.Profile != "" || c.Metastore.Glue.CredentialsFile != "" {
		cfg.Credentials = credentials.NewSharedCredentials(
			c.Metastore.Glue.CredentialsFile,
			c.Metastore.Glue.Profile,
		)
	}
	if c.Metastore.Glue.Credentials != nil {
		cfg.Credentials = credentials.NewStaticCredentials(
			c.Metastore.Glue.Credentials.AccessKeyID,
			c.Metastore.Glue.Credentials.AccessSecretKey,
			c.Metastore.Glue.Credentials.SessionToken,
		)
	}
	return cfg
}

func (c *Config) GetMetastoreHiveURI() string {
	return c.Metastore.Hive.URI
}

func (c *Config) GetMetastoreGlueCatalogID() string {
	return c.Metastore.Glue.CatalogID
}
func (c *Config) GetMetastoreType() string {
	return c.Metastore.Type
}
