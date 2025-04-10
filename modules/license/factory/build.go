package factory

import (
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/license"
)

func NewLicenseManager(_ config.Config) (license.Manager, error) {
	return &license.NopLicenseManager{}, nil
}
