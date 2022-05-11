package db_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/db"
	"github.com/treeverse/lakefs/pkg/db/params"
	"github.com/treeverse/lakefs/pkg/kv"
	kvpg "github.com/treeverse/lakefs/pkg/kv/postgres"
	"github.com/treeverse/lakefs/pkg/testutil"
)

const testMigrateValue = "This is a test value"

func TestKVMigration(t *testing.T) {
	ctx := context.Background()
	dbParams := params.Database{Driver: "pgx", ConnectionString: databaseURI, KVEnabled: true, DropTables: true}
	kvStore, err := kv.Open(ctx, kvpg.DriverName, dbParams.ConnectionString)
	testutil.MustDo(t, "Open KV store", err)
	tests := []struct {
		name       string
		migrations map[string]kvpg.MigrateFunc
		err        error
		entries    int
	}{
		{
			name:       "basic",
			migrations: map[string]kvpg.MigrateFunc{"basic": migrateBasic},
			err:        nil,
			entries:    5,
		},
		{
			name:       "parallel",
			migrations: map[string]kvpg.MigrateFunc{"basic": migrateBasic, "parallel": migrateParallel},
			err:        nil,
			entries:    10,
		},
		{
			name:       "empty",
			migrations: map[string]kvpg.MigrateFunc{"empty": migrateEmpty},
			err:        kv.ErrInvalidFormat,
		},
		{
			name:       "no_header",
			migrations: map[string]kvpg.MigrateFunc{"no_header": migrateNoHeader},
			err:        kv.ErrInvalidFormat,
		},
		{
			name:       "bad_entry",
			migrations: map[string]kvpg.MigrateFunc{"bad_entry": migrateBadEntry},
			err:        kv.ErrInvalidFormat,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kvpg.UnRegisterAll()
			for n, m := range tt.migrations {
				kvpg.Register(n, m, nil)
			}
			err = db.MigrateUp(dbParams)
			require.ErrorIs(t, err, tt.err)

			if tt.err == nil {
				data, err := kvStore.Get(ctx, []byte(kv.DBVersionPath))
				require.NoError(t, err)
				version, _ := strconv.Atoi(string(data))
				require.Equal(t, kv.InitialMigrateVersion, version)
				// revert migration version for next tests
				testutil.MustDo(t, "version rollback", kvStore.Set(ctx, []byte(kv.DBVersionPath), []byte("0")))
			}

			validate(ctx, t, kvStore, tt.entries)
			cleanup(ctx, t, kvStore)
		})
	}
}

func validate(ctx context.Context, t *testing.T, store kv.Store, entries int) {
	for i := 1; i <= entries; i++ {
		expectdVal := fmt.Sprint(i, ". ", testMigrateValue)
		value, err := store.Get(ctx, []byte(strconv.Itoa(i)))
		require.NoError(t, err)
		require.Equal(t, expectdVal, string(value))
	}
}

func cleanup(ctx context.Context, t *testing.T, store kv.Store) {
	t.Helper()
	scan, err := store.Scan(ctx, []byte{0})
	testutil.MustDo(t, "scan store", err)
	defer scan.Close()

	for scan.Next() {
		ent := scan.Entry()
		testutil.MustDo(t, "Clean store", store.Delete(ctx, ent.Key))
	}
}

// migrate functions for test scenarios

func migrateEmpty(_ context.Context, _ *pgxpool.Pool, _ io.Writer) error {
	return nil
}

func migrateBasic(_ context.Context, _ *pgxpool.Pool, writer io.Writer) error {
	jd := json.NewEncoder(writer)

	err := jd.Encode(kv.Header{
		Version:   kv.InitialMigrateVersion,
		Timestamp: time.Now(),
	})
	if err != nil {
		log.Fatal("Failed to encode struct")
	}
	for i := 1; i < 6; i++ {
		err = jd.Encode(kv.Entry{
			Key:   []byte(strconv.Itoa(i)),
			Value: []byte(fmt.Sprint(i, ". ", testMigrateValue)),
		})
		if err != nil {
			log.Fatal("Failed to encode struct")
		}
	}
	return nil
}

func migrateNoHeader(_ context.Context, _ *pgxpool.Pool, writer io.Writer) error {
	jd := json.NewEncoder(writer)

	for i := 1; i < 5; i++ {
		err := jd.Encode(kv.Entry{
			Key:   []byte(strconv.Itoa(i)),
			Value: []byte(fmt.Sprint(i, ". ", testMigrateValue)),
		})
		if err != nil {
			log.Fatal("Failed to encode struct")
		}
	}
	return nil
}

func migrateBadEntry(_ context.Context, _ *pgxpool.Pool, writer io.Writer) error {
	jd := json.NewEncoder(writer)

	err := jd.Encode(struct {
		key   []byte
		value int
	}{
		key:   []byte("test"),
		value: -1,
	})
	if err != nil {
		log.Fatal("Failed to encode struct")
	}
	return nil
}

func migrateParallel(_ context.Context, _ *pgxpool.Pool, writer io.Writer) error {
	jd := json.NewEncoder(writer)

	err := jd.Encode(kv.Header{
		Version:   kv.InitialMigrateVersion,
		Timestamp: time.Now(),
	})
	if err != nil {
		log.Fatal("Failed to encode struct")
	}

	for i := 6; i < 11; i++ {
		err := jd.Encode(kv.Entry{
			Key:   []byte(strconv.Itoa(i)),
			Value: []byte(fmt.Sprint(i, ". ", testMigrateValue)),
		})
		if err != nil {
			log.Fatal("Failed to encode struct")
		}
	}
	return nil
}
