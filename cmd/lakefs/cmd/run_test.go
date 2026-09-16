package cmd

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/crypt"
	"github.com/treeverse/lakefs/pkg/auth/model"
	authparams "github.com/treeverse/lakefs/pkg/auth/params"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
	"github.com/treeverse/lakefs/pkg/logging"
)

type stubRepositoryLister struct {
	repos []*catalog.Repository
}

func (s stubRepositoryLister) ListRepositories(context.Context, int, string, string, string, ...catalog.ListRepositoriesOptionsFunc) ([]*catalog.Repository, bool, error) {
	return s.repos, false, nil
}

func TestEnsureSetupComplete(t *testing.T) {
	repos := []*catalog.Repository{{Name: "repo"}}
	tests := []struct {
		name                  string
		alreadySetUp          bool
		repos                 []*catalog.Repository
		admin                 bool
		legacyUser            bool
		externalAuthorization bool
		expectedErr           error
		expectedSetUp         bool
	}{
		{name: "fresh installation"},
		{name: "administrator without repositories", admin: true, expectedSetUp: true},
		{name: "administrator with repositories", admin: true, repos: repos, expectedSetUp: true},
		{name: "already set up", alreadySetUp: true, repos: repos, expectedSetUp: true},
		{name: "repositories without administrator", repos: repos, expectedErr: errNoAdminUser},
		{name: "users of an earlier version", legacyUser: true, expectedErr: errNoAdminUser},
		{name: "external authorization service", externalAuthorization: true, expectedErr: errNoAdminUser},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()
			store := kvtest.GetStore(ctx, t)
			metadataManager := auth.NewKVMetadataManager("test", "installation", "mem", store)
			authService := auth.NewBasicAuthService(store, crypt.NewSecretStore([]byte("secret")),
				authparams.ServiceCache{}, logging.ContextUnavailable())
			if tt.alreadySetUp {
				require.NoError(t, metadataManager.UpdateSetupTimestamp(ctx, time.Now()))
			}
			if tt.admin {
				_, err := authService.CreateUser(ctx, &model.User{Username: "admin"})
				require.NoError(t, err)
			}
			if tt.legacyUser {
				require.NoError(t, kv.SetMsg(ctx, store, model.PartitionKey, model.UserPath("legacy"),
					model.ProtoFromUser(&model.User{Username: "legacy"})))
			}

			err := ensureSetupComplete(ctx, metadataManager, authService, store,
				stubRepositoryLister{repos: tt.repos}, tt.externalAuthorization)

			require.ErrorIs(t, err, tt.expectedErr)
			setUp, err := metadataManager.IsInitialized(ctx)
			require.NoError(t, err)
			require.Equal(t, tt.expectedSetUp, setUp)
		})
	}
}
