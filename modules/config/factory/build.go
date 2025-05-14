package factory

import (
	"github.com/treeverse/lakefs/pkg/config"
)

func GetServerName() string {
	return "lakeFS"
}

func BuildConfig(cfgType string) (config.Config, error) {
	c := &ConfigWithAuth{}
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
