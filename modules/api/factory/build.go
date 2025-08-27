package factory

import (
	"context"

	"github.com/go-chi/chi/v5"
	"github.com/gorilla/sessions"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/authentication"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/license"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/stats"
	"github.com/treeverse/lakefs/pkg/upload"
)

// BuildController returns a controller implementation for handling API requests
func BuildController(cfg config.Config, catalog *catalog.Catalog, authenticator auth.Authenticator, authService auth.Service, authenticationService authentication.Service, blockAdapter block.Adapter, metadataManager auth.MetadataManager, migrator api.Migrator, collector stats.Collector, actions api.ActionsHandler, auditChecker api.AuditChecker, logger logging.Logger, sessionStore sessions.Store, pathProvider upload.PathProvider, usageReporter stats.UsageReporterOperations, licenseManager license.Manager) apigen.ServerInterface {
	return api.NewController(cfg, catalog, authenticator, authService, authenticationService, blockAdapter, metadataManager, migrator, collector, actions, auditChecker, logger, sessionStore, pathProvider, usageReporter, licenseManager)
}

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
	return nil
}
