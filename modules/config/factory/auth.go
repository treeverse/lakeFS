package factory

import (
	"fmt"

	"github.com/treeverse/lakefs/pkg/config"
)

type ConfigWithAuth struct {
	config.BaseConfig `mapstructure:",squash"`
	Auth              config.Auth `mapstructure:"auth"`
}

func (c *ConfigWithAuth) AuthConfig() *config.Auth {
	return &c.Auth
}

func (c *ConfigWithAuth) Validate() error {
	missingKeys := config.ValidateMissingRequiredKeys(c, "mapstructure", "squash")
	if len(missingKeys) > 0 {
		return fmt.Errorf("%w: %v", config.ErrMissingRequiredKeys, missingKeys)
	}
	return config.ValidateBlockstore(&c.Blockstore)
}
