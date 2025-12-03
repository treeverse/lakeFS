package factory

import (
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/entensionhooks"
)

func BuildConfig(cfgType string) (config.Config, error) {
	if hook := entensionhooks.Get().ConfigBuilder; hook != nil {
		return hook.BuildConfig(cfgType)
	}
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
