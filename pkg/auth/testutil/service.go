package testutil

import (
	"context"
	"testing"

	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/crypt"
	authparams "github.com/treeverse/lakefs/pkg/auth/params"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
	"github.com/treeverse/lakefs/pkg/logging"
)

func SetupService(t *testing.T, ctx context.Context, secret []byte) *auth.AuthService {
	t.Helper()
	kvStore := kvtest.GetStore(ctx, t)
	return auth.NewAuthService(kvStore, crypt.NewSecretStore(secret), nil, authparams.ServiceCache{
		Enabled: false,
	}, logging.Default())
}
