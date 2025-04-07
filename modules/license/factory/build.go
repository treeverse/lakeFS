package factory

import (
	"github.com/treeverse/lakefs/pkg/license"
)

func NewNopLicenseManager() (license.Manager, error) {
	return license.NewNopLicenseManager(), nil
}
