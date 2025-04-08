package factory

import (
	"github.com/treeverse/lakefs/pkg/license"
)

func NewLicenseManager() (license.Manager, error) {
	return &license.NopLicenseManager{}, nil
}
