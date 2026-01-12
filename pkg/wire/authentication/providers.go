package authentication

import (
	"context"
	"fmt"
	"time"

	"github.com/goforj/wire"
	"github.com/treeverse/lakefs/pkg/auth"
	authremote "github.com/treeverse/lakefs/pkg/auth/remoteauthenticator"
	"github.com/treeverse/lakefs/pkg/authentication"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/logging"
)

// AuthenticationSet provides authentication service creation.
// External projects can replace this set to provide custom authentication implementations.
var AuthenticationSet = wire.NewSet(
	NewAuthenticationService,
	BuildAuthenticatorChain,
	NewLoginTokenProvider,
)

// NewAuthenticationService creates the authentication service based on configuration.
func NewAuthenticationService(_ context.Context, c config.Config, logger logging.Logger) (authentication.Service, error) {
	baseAuthCfg := c.AuthConfig().GetBaseAuthConfig()
	if baseAuthCfg.IsAuthenticationTypeAPI() {
		return authentication.NewAPIService(
			baseAuthCfg.AuthenticationAPI.Endpoint,
			baseAuthCfg.CookieAuthVerification.ValidateIDTokenClaims,
			logger.WithField("service", "authentication_api"),
			baseAuthCfg.AuthenticationAPI.ExternalPrincipalsEnabled)
	}
	return authentication.NewDummyService(), nil
}

// BuildAuthenticatorChain creates the authenticator chain for the gateway.
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

// UnimplementedLoginTokenProvider is the default login token provider for open-source.
type UnimplementedLoginTokenProvider struct{}

func (l UnimplementedLoginTokenProvider) GetRedirect(ctx context.Context) (*authentication.TokenRedirect, error) {
	return nil, authentication.ErrNotImplemented
}

func (l UnimplementedLoginTokenProvider) Release(ctx context.Context, loginRequestToken string) error {
	return authentication.ErrNotImplemented
}

func (l UnimplementedLoginTokenProvider) GetToken(ctx context.Context, mailbox string) (string, time.Time, error) {
	return "", time.Time{}, authentication.ErrNotImplemented
}

// NewLoginTokenProvider creates the login token provider.
func NewLoginTokenProvider(_ context.Context, _ config.Config, _ logging.Logger, _ kv.Store) (authentication.LoginTokenProvider, error) {
	return UnimplementedLoginTokenProvider{}, nil
}
