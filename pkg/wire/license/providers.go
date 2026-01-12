package license

import (
	"context"

	"github.com/goforj/wire"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/license"
)

// LicenseSet provides license manager creation.
// External projects can replace this set to provide custom license implementations.
var LicenseSet = wire.NewSet(
	NewLicenseManager,
)

// NewLicenseManager creates the license manager.
// The default open-source implementation returns a no-op license manager.
func NewLicenseManager(_ context.Context, _ config.Config) (license.Manager, error) {
	return &license.NopLicenseManager{}, nil
}
