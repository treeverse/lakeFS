package testutil

import (
	"context"
	"testing"

	"github.com/treeverse/lakefs/contrib/auth/acl"
	"github.com/treeverse/lakefs/pkg/auth/crypt"
	authparams "github.com/treeverse/lakefs/pkg/auth/params"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
)

func SetupService(t *testing.T, ctx context.Context, secret []byte) (*acl.AuthService, kv.Store) {
	t.Helper()
	kvStore := kvtest.GetStore(ctx, t)
	return acl.NewAuthService(kvStore, crypt.NewSecretStore(secret), authparams.ServiceCache{
		Enabled: false,
	}), kvStore
}
