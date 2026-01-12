package wire

import (
	"github.com/goforj/wire"
	wireapi "github.com/treeverse/lakefs/pkg/wire/api"
	wireauth "github.com/treeverse/lakefs/pkg/wire/auth"
	wireauthentication "github.com/treeverse/lakefs/pkg/wire/authentication"
	wireblock "github.com/treeverse/lakefs/pkg/wire/block"
	wirecatalog "github.com/treeverse/lakefs/pkg/wire/catalog"
	wireconfig "github.com/treeverse/lakefs/pkg/wire/config"
	wiregateway "github.com/treeverse/lakefs/pkg/wire/gateway"
	wirelicense "github.com/treeverse/lakefs/pkg/wire/license"
)

// DefaultSet is the complete open-source provider set.
// External projects can compose this set with their own overrides:
//
//	var CloudSet = wire.NewSet(
//	    wire.DefaultSet,
//	    cloudauth.CloudAuthSet,      // Replaces auth providers
//	    cloudlicense.CloudLicenseSet, // Replaces license providers
//	)
var DefaultSet = wire.NewSet(
	wireconfig.ConfigSet,
	wireauth.AuthSet,
	wireauthentication.AuthenticationSet,
	wireblock.BlockSet,
	wirecatalog.CatalogSet,
	wirelicense.LicenseSet,
	wiregateway.GatewaySet,
	wireapi.APISet,
)
