package config

import (
	"fmt"

	"github.com/goforj/wire"
	"github.com/treeverse/lakefs/pkg/config"
)

// ConfigSet provides configuration building.
// External projects can replace this set to provide custom configuration.
var ConfigSet = wire.NewSet(
	BuildConfig,
	wire.Bind(new(config.Config), new(*ConfigImpl)),
)

// ConfigImpl is the open-source lakeFS configuration implementation.
type ConfigImpl struct {
	config.BaseConfig `mapstructure:",squash"`
	Auth              config.Auth `mapstructure:"auth"`
	UI                config.UI   `mapstructure:"ui"`
}

func (c *ConfigImpl) AuthConfig() config.AuthConfig {
	return &c.Auth
}

func (c *ConfigImpl) UIConfig() config.UIConfig {
	return &c.UI
}

func (c *ConfigImpl) Validate() error {
	missingKeys := config.ValidateMissingRequiredKeys(c, "mapstructure", "squash")
	if len(missingKeys) > 0 {
		return fmt.Errorf("%w: %v", config.ErrMissingRequiredKeys, missingKeys)
	}
	return config.ValidateBlockstore(&c.Blockstore)
}

// BuildConfig creates and validates the configuration.
func BuildConfig(cfgType string) (*ConfigImpl, error) {
	c := &ConfigImpl{}
	_, err := config.NewConfig(cfgType, c)
	if err != nil {
		return nil, err
	}

	// Perform required validations
	if err = c.Validate(); err != nil {
		return nil, err
	}

	err = c.ValidateDomainNames()
	if err != nil {
		return nil, err
	}

	return c, nil
}
