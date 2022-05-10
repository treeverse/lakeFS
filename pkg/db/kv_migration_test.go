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

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/db"
	"github.com/treeverse/lakefs/pkg/db/params"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/export"
	"github.com/treeverse/lakefs/pkg/kv/postgres"
	"github.com/treeverse/lakefs/pkg/testutil"
)

const testValue = "This is a test value"

func TestKVMigration(t *testing.T) {
	ctx := context.Background()
	dbParams := params.Database{Driver: "pgx", ConnectionString: databaseURI, KVEnabled: true}
	kvStore, err := kv.Open(ctx, postgres.DriverName, dbParams.ConnectionString)
	testutil.MustDo(t, "Open KV store", err)
	tests := []struct {
		name       string
		migrations map[string]db.MigrateFunc
		err        error
		entries    int
	}{
		{
			name:       "basic",
			migrations: map[string]db.MigrateFunc{"basic": migrateBasic},
			err:        nil,
			entries:    5,
		},
		{
			name:       "parallel",
			migrations: map[string]db.MigrateFunc{"basic": migrateBasic, "parallel": migrateParallel},
			err:        nil,
			entries:    10,
		},
		{
			name:       "empty",
			migrations: map[string]db.MigrateFunc{"empty": migrateEmpty},
			err:        export.ErrEmptyFile,
		},
		{
			name:       "no_header",
			migrations: map[string]db.MigrateFunc{"no_header": migrateNoHeader},
			err:        export.ErrBadHeader,
		},
		{
			name:       "bad_entry",
			migrations: map[string]db.MigrateFunc{"bad_entry": migrateBadEntry},
			err:        export.ErrBadHeader,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db.KVUnRegisterAll()
			for n, m := range tt.migrations {
				db.KVRegister(n, m)
			}
			err = db.MigrateUp(dbParams, true)
			require.ErrorIs(t, err, tt.err)

			if tt.err == nil {
				dbPool, _ := testutil.GetDB(t, databaseURI)
				version, dirty, err := db.MigrateVersion(ctx, dbPool, dbParams)
				require.NoError(t, err)
				require.False(t, dirty)
				require.Equal(t, uint(kv.MigrateVersion+1), version)
				// revert migration version for next tests
				testutil.MustDo(t, "version rollback", db.MigrateTo(ctx, dbParams, kv.MigrateVersion, true))
			}

			validate(ctx, t, kvStore, tt.entries)
			cleanup(ctx, t, kvStore)
		})
	}
}

func validate(ctx context.Context, t *testing.T, store kv.Store, entries int) {
	for i := 1; i <= entries; i++ {
		expectdVal := fmt.Sprint(i, ". ", testValue)
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

func migrateEmpty(_ db.Database, _ io.Writer) error {
	return nil
}

func migrateBasic(_ db.Database, writer io.Writer) error {
	jd := json.NewEncoder(writer)

	err := jd.Encode(export.Header{
		Version:   kv.MigrateVersion + 1,
		Timestamp: time.Now(),
	})
	if err != nil {
		log.Fatal("Failed to encode struct")
	}
	for i := 1; i < 6; i++ {
		err = jd.Encode(export.Entry{
			Key:   []byte(strconv.Itoa(i)),
			Value: []byte(fmt.Sprint(i, ". ", testValue)),
		})
		if err != nil {
			log.Fatal("Failed to encode struct")
		}
	}
	return nil
}

func migrateNoHeader(_ db.Database, writer io.Writer) error {
	jd := json.NewEncoder(writer)

	for i := 1; i < 5; i++ {
		err := jd.Encode(export.Entry{
			Key:   []byte(strconv.Itoa(i)),
			Value: []byte(fmt.Sprint(i, ". ", testValue)),
		})
		if err != nil {
			log.Fatal("Failed to encode struct")
		}
	}
	return nil
}

func migrateBadEntry(_ db.Database, writer io.Writer) error {
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

func migrateParallel(_ db.Database, writer io.Writer) error {
	jd := json.NewEncoder(writer)

	err := jd.Encode(export.Header{
		Version:   kv.MigrateVersion + 1,
		Timestamp: time.Now(),
	})
	if err != nil {
		log.Fatal("Failed to encode struct")
	}

	for i := 6; i < 11; i++ {
		err := jd.Encode(export.Entry{
			Key:   []byte(strconv.Itoa(i)),
			Value: []byte(fmt.Sprint(i, ". ", testValue)),
		})
		if err != nil {
			log.Fatal("Failed to encode struct")
		}
	}
	return nil
}
