package acl

import (
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/mitchellh/mapstructure"
	"github.com/spf13/viper"
	"github.com/treeverse/lakefs/pkg/config"
)

const DefaultListenAddress = "0.0.0.0:8001"

var (
	ErrBadConfiguration = errors.New("bad configuration")
	ErrMissingSecretKey = fmt.Errorf("%w: encrypt.secret_key cannot be empty", ErrBadConfiguration)
)

type HTTPClient struct {
	SkipVerify bool          `mapstructure:"skip_verify"`
	Timeout    time.Duration `mapstructure:"timeout"`
}

type Config struct {
	ListenAddress string `mapstructure:"listen_address"`

	Database config.Database
	Encrypt  struct {
		SecretKey config.SecureString `mapstructure:"secret_key" validate:"required"`
	}
	Cache struct {
		Enabled bool
		Size    int
		TTL     time.Duration
		Jitter  time.Duration
	} `mapstructure:"cache"`
}

func NewConfig() (*Config, error) {
	c := &Config{}

	// Inform viper of all expected fields.  Otherwise, it fails to deserialize from the
	// environment.
	keys := config.GetStructKeys(reflect.TypeOf(c), "mapstructure", "squash")
	for _, key := range keys {
		viper.SetDefault(key, nil)
	}
	setDefaults()
	err := Unmarshal(c)
	return c, err
}

func Unmarshal(c *Config) error {
	return viper.UnmarshalExact(&c,
		viper.DecodeHook(
			mapstructure.ComposeDecodeHookFunc(
				config.DecodeStrings, mapstructure.StringToTimeDurationHookFunc())))
}

func (c *Config) Validate() error {
	missingKeys := config.ValidateMissingRequiredKeys(c, "mapstructure", "squash")
	if len(missingKeys) > 0 {
		return fmt.Errorf("%w: %v", config.ErrMissingRequiredKeys, missingKeys)
	}
	return nil
}

func (c *Config) AuthEncryptionSecret() []byte {
	secret := c.Encrypt.SecretKey
	if len(secret) == 0 {
		panic(fmt.Errorf("%w. Please set it to a unique, randomly generated value and store it somewhere safe", ErrMissingSecretKey))
	}
	return []byte(secret)
}

//nolint:mnd
func setDefaults() {
	viper.SetDefault("listen_address", DefaultListenAddress)
	viper.SetDefault("cache.enabled", true)
	viper.SetDefault("cache.size", 1024)
	viper.SetDefault("cache.ttl", 20*time.Second)
	viper.SetDefault("cache.jitter", 3*time.Second)

	viper.SetDefault("database.local.path", "~/aclserver/metadata")
	viper.SetDefault("database.local.prefetch_size", 256)
	viper.SetDefault("database.local.sync_writes", true)

	viper.SetDefault("database.dynamodb.table_name", "kvstore")
	viper.SetDefault("database.dynamodb.scan_limit", 1024)

	viper.SetDefault("database.postgres.max_open_connections", 25)
	viper.SetDefault("database.postgres.max_idle_connections", 25)
	viper.SetDefault("database.postgres.connection_max_lifetime", "5m")
}
