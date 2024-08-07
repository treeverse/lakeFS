package testutil

import (
	"context"
	"testing"

	"github.com/treeverse/lakefs/contrib/auth-acl"
	"github.com/treeverse/lakefs/pkg/auth/crypt"
	authparams "github.com/treeverse/lakefs/pkg/auth/params"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
	"github.com/treeverse/lakefs/pkg/logging"
)

func SetupService(t *testing.T, ctx context.Context, secret []byte) (*auth_acl.AuthService, kv.Store) {
	t.Helper()
	kvStore := kvtest.GetStore(ctx, t)
	return auth_acl.NewAuthService(kvStore, crypt.NewSecretStore(secret), authparams.ServiceCache{
		Enabled: false,
	}, logging.ContextUnavailable()), kvStore
}
