package api

import (
	"context"
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/goforj/wire"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/authentication"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/icebergsync"
	"github.com/treeverse/lakefs/pkg/license"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/stats"
)

// APISet provides API-related factory functions.
// External projects can replace this set to provide custom API implementations.
var APISet = wire.NewSet(
	NewIcebergSyncController,
)

// ServiceDependencies holds all dependencies needed by API services.
type ServiceDependencies struct {
	Config                config.Config
	AuthService           auth.Service
	Authenticator         auth.Authenticator
	AuthenticationService authentication.Service
	BlockAdapter          block.Adapter
	Collector             stats.Collector
	Logger                logging.Logger
	LicenseManager        license.Manager
}

// ProvideServiceDependencies creates the service dependencies struct.
func ProvideServiceDependencies(
	cfg config.Config,
	authService auth.Service,
	authenticator auth.Authenticator,
	authenticationService authentication.Service,
	blockAdapter block.Adapter,
	collector stats.Collector,
	logger logging.Logger,
	licenseManager license.Manager,
) ServiceDependencies {
	return ServiceDependencies{
		Config:                cfg,
		AuthService:           authService,
		Authenticator:         authenticator,
		AuthenticationService: authenticationService,
		BlockAdapter:          blockAdapter,
		Collector:             collector,
		Logger:                logger,
		LicenseManager:        licenseManager,
	}
}

// RegisterServices registers additional API services on the router.
// The default open-source implementation registers placeholder Iceberg endpoints.
func RegisterServices(_ context.Context, _ ServiceDependencies, router *chi.Mux) error {
	// Additional API routes we like to serve and report as not implemented
	router.Mount("/iceberg/api/", http.HandlerFunc(NotImplementedIcebergCatalogHandler))
	router.Mount("/iceberg/relative_to/", http.HandlerFunc(NotImplementedIcebergCatalogHandler))
	router.Mount("/mds/iceberg/api/", http.HandlerFunc(NotImplementedIcebergCatalogHandler))

	return nil
}

// NotImplementedIcebergCatalogHandler returns HTTP 501 Not Implemented status for Iceberg REST Catalog endpoints.
func NotImplementedIcebergCatalogHandler(w http.ResponseWriter, _ *http.Request) {
	http.Error(w, "Iceberg REST Catalog Not Implemented", http.StatusNotImplemented)
}

// BuildConditionFromParams creates a graveler.ConditionFunc from upload params.
// Returns nil if no precondition is specified in the params.
// Handles IfNoneMatch (must be "*") and IfMatch (ETag validation).
func BuildConditionFromParams(ifMatch, ifNoneMatch *string) (*graveler.ConditionFunc, error) {
	var condition graveler.ConditionFunc
	switch {
	case ifMatch != nil && ifNoneMatch != nil:
		return nil, fmt.Errorf("cannot specify both If-Match and If-None-Match: %w", catalog.ErrNotImplemented)
	case ifMatch != nil:
		// Handle IfMatch: not supported
		return nil, catalog.ErrNotImplemented
	case ifNoneMatch != nil && *ifNoneMatch != "*":
		// If-None-Match only supports "*"
		return nil, fmt.Errorf("If-None-Match only supports '*': %w", catalog.ErrNotImplemented)
	case ifNoneMatch != nil:
		condition = func(currentValue *graveler.Value) error {
			if currentValue != nil {
				return graveler.ErrPreconditionFailed
			}
			return nil
		}
	}
	return &condition, nil
}

// NewIcebergSyncController creates the Iceberg sync controller.
// The default open-source implementation returns a no-op controller.
func NewIcebergSyncController(_ config.Config) icebergsync.Controller {
	return &icebergsync.NopController{}
}
