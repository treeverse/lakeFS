package factory

import (
	"context"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/authentication"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/license"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/stats"
)

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

func RegisterServices(ctx context.Context, sd ServiceDependencies, router *chi.Mux) error {
	// Additional API routes we like to serve and report as not implemented
	router.Mount("/iceberg/api/", http.HandlerFunc(NotImplementedIcebergCatalogHandler))
	router.Mount("/iceberg/relative_to/", http.HandlerFunc(NotImplementedIcebergCatalogHandler))
	router.Mount("/mds/iceberg/api/", http.HandlerFunc(NotImplementedIcebergCatalogHandler))

	return nil
}

// NotImplementedIcebergCatalogHandler returns HTTP 501 Not Implemented status for Iceberg REST Catalog endpoints.
func NotImplementedIcebergCatalogHandler(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "Iceberg REST Catalog Not Implemented", http.StatusNotImplemented)
}

// BuildConditionFromParams creates a graveler.ConditionFunc from upload params.
// Returns nil if no precondition is specified in the params.
func BuildConditionFromParams(params apigen.UploadObjectParams) *graveler.ConditionFunc {
	return nil
}
