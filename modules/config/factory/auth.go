package factory

import (
	"fmt"

	"github.com/treeverse/lakefs/pkg/config"
)

type Config struct {
	config.BaseConfig `mapstructure:",squash"`
	Auth              config.Auth `mapstructure:"auth"`
	UI                config.UI   `mapstructure:"ui"`
}

func (c *Config) AuthConfig() *config.Auth {
	return &c.Auth
}

func (c *Config) UIConfig() config.UIConfig {
	return &c.UI
}

func (c *Config) Validate() error {
	missingKeys := config.ValidateMissingRequiredKeys(c, "mapstructure", "squash")
	if len(missingKeys) > 0 {
		return fmt.Errorf("%w: %v", config.ErrMissingRequiredKeys, missingKeys)
	}
	return config.ValidateBlockstore(&c.Blockstore)
}
