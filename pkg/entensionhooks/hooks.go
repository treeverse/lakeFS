package entensionhooks

import (
	"context"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/authentication"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/icebergsync"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/license"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/stats"
)

// Hooks defines optional enterprise-provided implementations used by the OSS runtime.
// OSS defaults remain active when a hook is nil.
type Hooks struct {
	GatewayMiddleware GatewayMiddlewareHook
	AuthService       AuthServiceHook
	Authentication    AuthenticationHook
	API               APIHook
	BlockAdapter      BlockAdapterHook
	Catalog           CatalogHook
	ConfigBuilder     ConfigBuilderHook
	LicenseManager    LicenseManagerHook
}

// GatewayMiddlewareHook allows the enterprise distribution to inject S3 gateway middleware.
type GatewayMiddlewareHook interface {
	Build(ctx context.Context, cfg config.Config, logger logging.Logger) (func(http.Handler) http.Handler, error)
}

type AuthServiceHook interface {
	New(ctx context.Context, cfg config.Config, logger logging.Logger, kvStore kv.Store, metadataManager *auth.KVMetadataManager) (auth.Service, error)
}

type AuthenticationHook interface {
	NewAuthenticationService(ctx context.Context, cfg config.Config, logger logging.Logger) (authentication.Service, error)
	BuildAuthenticatorChain(cfg config.Config, logger logging.Logger, authService auth.Service) (auth.ChainAuthenticator, error)
}

// ServiceDependencies is a minimal set of API dependencies exposed for enterprise registration.
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

type APIHook interface {
	RegisterServices(ctx context.Context, sd ServiceDependencies, router *chi.Mux) error
	BuildConditionFromParams(ifMatch, ifNoneMatch *string) (*graveler.ConditionFunc, error)
	NewIcebergSyncController(cfg config.Config) icebergsync.Controller
}

type BlockAdapterHook interface {
	Build(ctx context.Context, statsCollector stats.Collector, cfg config.Config) (block.Adapter, error)
}

type CatalogHook interface {
	BuildConflictResolvers(cfg config.Config, adapter block.Adapter) []graveler.ConflictResolver
}

type ConfigBuilderHook interface {
	BuildConfig(cfgType string) (config.Config, error)
}

type LicenseManagerHook interface {
	New(ctx context.Context, cfg config.Config) (license.Manager, error)
}

var hooks Hooks

// Set overrides the active hooks. Intended to be called once at process init by enterprise.
func Set(h Hooks) {
	hooks = h
}

// Get returns the currently registered hooks.
func Get() Hooks {
	return hooks
}
