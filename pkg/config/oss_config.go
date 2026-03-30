package config

import (
	"fmt"
)

type ConfigImpl struct {
	BaseConfig `mapstructure:",squash"`
	Auth       Auth `mapstructure:"auth"`
	UI         UI   `mapstructure:"ui"`
}

func (c *ConfigImpl) AuthConfig() AuthConfig {
	return &c.Auth
}

func (c *ConfigImpl) UIConfig() UIConfig {
	return &c.UI
}

func (c *ConfigImpl) Validate() error {
	missingKeys := ValidateMissingRequiredKeys(c, "mapstructure", "squash")
	if len(missingKeys) > 0 {
		return fmt.Errorf("%w: %v", ErrMissingRequiredKeys, missingKeys)
	}
	return ValidateBlockstore(&c.Blockstore)
}

func BuildConfig(cfgType string) (Config, error) {
	c := &ConfigImpl{}
	_, err := NewConfig(cfgType, c)
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
