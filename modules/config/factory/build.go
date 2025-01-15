package factory

import (
	"github.com/treeverse/lakefs/pkg/config"
)

func BuildConfig(cfgType string) (config.Interface, error) {
	return newConfig(cfgType)
}

func newConfig(cfgType string) (*config.Config, error) {
	c := &config.Config{}
	c, err := config.NewConfig(cfgType, c)
	if err != nil {
		return nil, err
	}

	// OSS specific validation
	err = c.ValidateDomainNames()
	if err != nil {
		return nil, err
	}

	return c, nil
}
