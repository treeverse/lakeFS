package factory

import (
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/logging"
)

func BuildAuthenticatorChain(_ config.Config, _ logging.Logger, _ auth.Service) auth.ChainAuthenticator {
	return auth.ChainAuthenticator{}
}
