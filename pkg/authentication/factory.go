package authentication

import (
	"context"
	"fmt"

	"github.com/treeverse/lakefs/pkg/auth"
	authremote "github.com/treeverse/lakefs/pkg/auth/remoteauthenticator"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/logging"
)

func NewAuthenticationService(_ context.Context, c config.Config, logger logging.Logger) (Service, error) {
	baseAuthCfg := c.AuthConfig().GetBaseAuthConfig()
	if baseAuthCfg.IsAuthenticationTypeAPI() {
		return NewAPIService(
			baseAuthCfg.AuthenticationAPI.Endpoint,
			baseAuthCfg.CookieAuthVerification.ValidateIDTokenClaims,
			logger.WithField("service", "authentication_api"),
			baseAuthCfg.AuthenticationAPI.ExternalPrincipalsEnabled)
	}
	return NewDummyService(), nil
}

func BuildAuthenticatorChain(c config.Config, logger logging.Logger, authService auth.Service) (auth.ChainAuthenticator, error) {
	authCfg := c.AuthConfig()
	baseAuthCfg := authCfg.GetBaseAuthConfig()
	authenticators := auth.ChainAuthenticator{
		auth.NewBuiltinAuthenticator(authService),
	}

	// remote authenticator setup
	if baseAuthCfg.RemoteAuthenticator.Enabled {
		remoteAuthenticator, err := authremote.NewAuthenticator(
			authremote.AuthenticatorConfig(baseAuthCfg.RemoteAuthenticator),
			authService,
			logger)
		if err != nil {
			return authenticators, fmt.Errorf("failed to create remote authenticator: %w", err)
		}

		authenticators = append(authenticators, remoteAuthenticator)
	}

	return authenticators, nil
}
