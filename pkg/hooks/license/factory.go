package factory

import (
	"context"

	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/entensionhooks"
	"github.com/treeverse/lakefs/pkg/license"
)

func NewLicenseManager(ctx context.Context, cfg config.Config) (license.Manager, error) {
	if hook := entensionhooks.Get().LicenseManager; hook != nil {
		return hook.New(ctx, cfg)
	}
	return &license.NopLicenseManager{}, nil
}
