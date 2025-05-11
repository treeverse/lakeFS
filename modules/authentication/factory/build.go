package factory

import (
	"context"
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/gorilla/sessions"
	"github.com/treeverse/lakefs/pkg/auth"
	authremote "github.com/treeverse/lakefs/pkg/auth/remoteauthenticator"
	"github.com/treeverse/lakefs/pkg/authentication"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/logging"
)

type nopOidcProvider struct {
	logger logging.Logger
}

func (n *nopOidcProvider) RegisterOIDCRoutes(_ *chi.Mux, _ sessions.Store) {
	n.logger.Warn("OIDC is not implemented")
}

func (n *nopOidcProvider) OIDCCallback(w http.ResponseWriter, _ *http.Request, _ sessions.Store) {
	n.logger.Warn("OIDC is not implemented")
	w.WriteHeader(http.StatusBadRequest)
}

func NewOIDCProvider(_ context.Context, _ config.Config, logger logging.Logger) (authentication.OIDCProvider, error) {
	return &nopOidcProvider{
		logger: logger,
	}, nil
}

func BuildAuthenticatorChain(c config.Config, logger logging.Logger, authService auth.Service) (auth.ChainAuthenticator, error) {
	authConfig := c.AuthConfig()

	authenticators := auth.ChainAuthenticator{
		auth.NewBuiltinAuthenticator(authService),
	}

	// remote authenticator setup
	if authConfig.RemoteAuthenticator.Enabled {
		remoteAuthenticator, err := authremote.NewAuthenticator(authremote.AuthenticatorConfig(authConfig.RemoteAuthenticator), authService, logger)
		if err != nil {
			return authenticators, fmt.Errorf("failed to create remote authenticator: %w", err)

		}

		authenticators = append(authenticators, remoteAuthenticator)
	}

	return authenticators, nil
}
