package db_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/db"
	"github.com/treeverse/lakefs/pkg/db/params"
	"github.com/treeverse/lakefs/pkg/kv"
	kvparams "github.com/treeverse/lakefs/pkg/kv/params"
	kvpg "github.com/treeverse/lakefs/pkg/kv/postgres"
	"github.com/treeverse/lakefs/pkg/testutil"
)

func TestKVMigration(t *testing.T) {
	ctx := context.Background()
	dbParams := params.Database{Type: kvpg.DriverName, Driver: "pgx", ConnectionString: databaseURI, DropTables: true}
	kvParams := kvparams.KV{Type: kvpg.DriverName, Postgres: &kvparams.Postgres{ConnectionString: dbParams.ConnectionString}}
	kvStore, err := kv.Open(ctx, kvParams)
	require.NoError(t, err)
	defer kvStore.Close()

	testutil.MustDo(t, "Open KV store", err)
	tests := []struct {
		name       string
		migrations map[string]kvpg.MigrateFunc
		err        error
		entries    int
	}{
		{
			name:       "basic",
			migrations: map[string]kvpg.MigrateFunc{"basic": testutil.MigrateBasic},
			err:        nil,
			entries:    5,
		},
		{
			name:       "parallel",
			migrations: map[string]kvpg.MigrateFunc{"basic": testutil.MigrateBasic, "parallel": testutil.MigrateParallel},
			err:        nil,
			entries:    10,
		},
		{
			name:       "empty",
			migrations: map[string]kvpg.MigrateFunc{"empty": testutil.MigrateEmpty},
			err:        kv.ErrInvalidFormat,
		},
		{
			name:       "no_header",
			migrations: map[string]kvpg.MigrateFunc{"no_header": testutil.MigrateNoHeader},
			err:        kv.ErrInvalidFormat,
		},
		{
			name:       "bad_entry",
			migrations: map[string]kvpg.MigrateFunc{"bad_entry": testutil.MigrateBadEntry},
			err:        kv.ErrInvalidFormat,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kvpg.UnregisterAll()
			for n, m := range tt.migrations {
				kvpg.RegisterMigrate(n, m, nil)
			}
			err = db.MigrateUp(dbParams, nil, kvParams)
			require.ErrorIs(t, err, tt.err)

			if tt.err == nil {
				version, err := kv.GetDBSchemaVersion(ctx, kvStore)
				require.NoError(t, err)
				require.Equal(t, kv.InitialMigrateVersion, version)
			}

			testutil.ValidateKV(ctx, t, kvStore, tt.entries)
			testutil.CleanupKV(ctx, t, kvStore)
			kvpg.UnregisterAll()
		})
	}
}
