package factory

import (
	"context"
	"fmt"
	"time"

	"github.com/treeverse/lakefs/pkg/auth"
	authremote "github.com/treeverse/lakefs/pkg/auth/remoteauthenticator"
	"github.com/treeverse/lakefs/pkg/authentication"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/entensionhooks"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/logging"
)

// NewAuthenticationService returns the OSS authentication service or an enterprise override.
func NewAuthenticationService(ctx context.Context, c config.Config, logger logging.Logger) (authentication.Service, error) {
	if hook := entensionhooks.Get().Authentication; hook != nil {
		return hook.NewAuthenticationService(ctx, c, logger)
	}
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

// BuildAuthenticatorChain builds the chain of authenticators, allowing enterprise override.
func BuildAuthenticatorChain(c config.Config, logger logging.Logger, authService auth.Service) (auth.ChainAuthenticator, error) {
	if hook := entensionhooks.Get().Authentication; hook != nil {
		return hook.BuildAuthenticatorChain(c, logger, authService)
	}
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

// Login token provider is not implemented in OSS; enterprise can override via hooks.
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

func NewLoginTokenProvider(_ context.Context, c config.Config, logger logging.Logger, store kv.Store) (authentication.LoginTokenProvider, error) {
	return UnimplementedLoginTokenProvider{}, nil
}
