package factory

import (
	"fmt"

	"github.com/treeverse/lakefs/pkg/auth"
	authremote "github.com/treeverse/lakefs/pkg/auth/remoteauthenticator"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/logging"
)

func BuildAuthenticatorChain(c config.Config, logger logging.Logger, authService auth.Service) (auth.ChainAuthenticator, error) {
	baseConfig := c.GetBaseConfig()

	authenticators := auth.ChainAuthenticator{
		auth.NewBuiltinAuthenticator(authService),
	}

	// remote authenticator setup
	if baseConfig.Auth.RemoteAuthenticator.Enabled {
		remoteAuthenticator, err := authremote.NewAuthenticator(authremote.AuthenticatorConfig(baseConfig.Auth.RemoteAuthenticator), authService, logger)
		if err != nil {
			return authenticators, fmt.Errorf("failed to create remote authenticator: %w", err)

		}

		authenticators = append(authenticators, remoteAuthenticator)
	}

	return authenticators, nil
}
