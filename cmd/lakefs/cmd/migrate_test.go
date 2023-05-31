package cmd_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/cmd/lakefs/cmd"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
)

func TestDoMigrate(t *testing.T) {
	ctx := context.Background()

	t.Run("not_initialized", func(t *testing.T) {
		kvStore := kvtest.GetStore(ctx, t)
		err := cmd.DoMigration(ctx, kvStore, nil, false)
		require.ErrorIs(t, err, kv.ErrNotFound)
		_, err = kv.GetDBSchemaVersion(ctx, kvStore)
		require.ErrorIs(t, err, kv.ErrNotFound)
	})

	t.Run("not_meets_required_version", func(t *testing.T) {
		kvStore := kvtest.GetStore(ctx, t)
		require.NoError(t, kv.SetDBSchemaVersion(ctx, kvStore, kv.InitialMigrateVersion))
		err := cmd.DoMigration(ctx, kvStore, nil, false)
		require.ErrorIs(t, err, kv.ErrMigrationVersion)
		version, err := kv.GetDBSchemaVersion(ctx, kvStore)
		require.NoError(t, err)
		require.Equal(t, kv.InitialMigrateVersion, version)
	})

	t.Run("from_acl_v1_no_force", func(t *testing.T) {
		cfg := config.Config{}
		cfg.Auth.UIConfig.RBAC = config.AuthRBACSimplified
		kvStore := kvtest.GetStore(ctx, t)
		require.NoError(t, kv.SetDBSchemaVersion(ctx, kvStore, kv.ACLMigrateVersion))
		err := cmd.DoMigration(ctx, kvStore, &cfg, false)
		require.ErrorIs(t, err, kv.ErrMigrationVersion)
		version, err := kv.GetDBSchemaVersion(ctx, kvStore)
		require.NoError(t, err)
		require.Equal(t, kv.ACLMigrateVersion, version)
	})

	t.Run("from_acl_v1_force", func(t *testing.T) {
		cfg := config.Config{}
		cfg.Auth.UIConfig.RBAC = config.AuthRBACSimplified
		kvStore := kvtest.GetStore(ctx, t)
		require.NoError(t, kv.SetDBSchemaVersion(ctx, kvStore, kv.ACLNoReposMigrateVersion))
		err := cmd.DoMigration(ctx, kvStore, &cfg, true)
		require.NoError(t, err)
		version, err := kv.GetDBSchemaVersion(ctx, kvStore)
		require.NoError(t, err)
		require.True(t, kv.IsLatestSchemaVersion(version))
	})

	t.Run("from_acl_v2", func(t *testing.T) {
		cfg := config.Config{}
		cfg.Auth.UIConfig.RBAC = config.AuthRBACSimplified
		startVer := kv.ACLNoReposMigrateVersion
		for !kv.IsLatestSchemaVersion(startVer) {
			kvStore := kvtest.GetStore(ctx, t)
			require.NoError(t, kv.SetDBSchemaVersion(ctx, kvStore, uint(startVer)))
			err := cmd.DoMigration(ctx, kvStore, &cfg, false)
			require.NoError(t, err)
			version, err := kv.GetDBSchemaVersion(ctx, kvStore)
			require.NoError(t, err)
			require.True(t, kv.IsLatestSchemaVersion(version))
			startVer += 1
		}
	})

	t.Run("latest_version", func(t *testing.T) {
		cfg := config.Config{}
		cfg.Auth.UIConfig.RBAC = config.AuthRBACSimplified
		kvStore := kvtest.GetStore(ctx, t)
		require.NoError(t, kv.SetDBSchemaVersion(ctx, kvStore, kv.NextSchemaVersion-1))
		err := cmd.DoMigration(ctx, kvStore, &cfg, false)
		require.NoError(t, err)
		version, err := kv.GetDBSchemaVersion(ctx, kvStore)
		require.NoError(t, err)
		require.True(t, kv.IsLatestSchemaVersion(version))
	})

	t.Run("next_version", func(t *testing.T) {
		cfg := config.Config{}
		cfg.Auth.UIConfig.RBAC = config.AuthRBACSimplified
		kvStore := kvtest.GetStore(ctx, t)
		require.NoError(t, kv.SetDBSchemaVersion(ctx, kvStore, kv.NextSchemaVersion))
		err := cmd.DoMigration(ctx, kvStore, &cfg, false)
		require.ErrorIs(t, err, kv.ErrMigrationVersion)
		version, err := kv.GetDBSchemaVersion(ctx, kvStore)
		require.NoError(t, err)
		require.Equal(t, kv.NextSchemaVersion, version)
	})

	t.Run("invalid_version", func(t *testing.T) {
		cfg := config.Config{}
		cfg.Auth.UIConfig.RBAC = config.AuthRBACSimplified
		kvStore := kvtest.GetStore(ctx, t)
		require.NoError(t, kv.SetDBSchemaVersion(ctx, kvStore, 0))
		err := cmd.DoMigration(ctx, kvStore, &cfg, false)
		require.ErrorIs(t, err, kv.ErrMigrationVersion)
		version, err := kv.GetDBSchemaVersion(ctx, kvStore)
		require.NoError(t, err)
		require.Equal(t, 0, version)
	})
}
