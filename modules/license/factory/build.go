package factory

import (
	"context"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/license"
)

func NewLicenseManager(_ context.Context, _ config.Config) (license.Manager, error) {
	return &license.NopLicenseManager{}, nil
}
