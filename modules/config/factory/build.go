package factory

import (
	"github.com/treeverse/lakefs/pkg/config"
)

func BuildConfig(cfgType string) (config.Interface, error) {
	c := &config.BaseConfig{}
	c, err := config.NewConfig(cfgType, c)
	if err != nil {
		return nil, err
	}

	// Perform required validations
	err = c.ValidateDomainNames()
	if err != nil {
		return nil, err
	}

	return c, nil
}
