package factory

import (
	"github.com/treeverse/lakefs/pkg/auth"
	ossconfig "github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/logging"
)

func AdditionalAuthenticators(_ ossconfig.Config, _ logging.Logger) auth.ChainAuthenticator {
	return auth.ChainAuthenticator{}
}
