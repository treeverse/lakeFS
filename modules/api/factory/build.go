package factory

import (
	"context"

	"github.com/go-chi/chi/v5"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/authentication"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/config"
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
	return nil
}
