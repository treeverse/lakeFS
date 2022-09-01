package postgres

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/treeverse/lakefs/pkg/db/params"
	"github.com/treeverse/lakefs/pkg/kv"
	kvparams "github.com/treeverse/lakefs/pkg/kv/params"
	"github.com/treeverse/lakefs/pkg/logging"
)

var ErrAlreadyMigrated = errors.New("already migrated")

type MigrateFunc func(ctx context.Context, db *pgxpool.Pool, writer io.Writer) error

var (
	kvPkgs   = make(map[string]pkgMigrate)
	kvPkgsMu sync.RWMutex
)

type pkgMigrate struct {
	Func   MigrateFunc
	Tables []string
}

func timeTrack(start time.Time, logger logging.Logger, name string) {
	elapsed := time.Since(start)
	logger.Infof("%s took %s", name, elapsed)
}

// Migrate data migration from DB to KV
func Migrate(ctx context.Context, dbPool *pgxpool.Pool, dbParams params.Database) error {
	if !dbParams.KVEnabled {
		return nil
	}

	kvParams := kvparams.KV{
		Type:     DriverName,
		Postgres: &kvparams.Postgres{ConnectionString: dbParams.ConnectionString},
	}
	store, err := kv.Open(ctx, kvParams)
	if err != nil {
		return fmt.Errorf("opening kv store: %w", err)
	}
	defer store.Close()

	shouldDrop, err := getMigrationStatus(ctx, store)
	if err != nil {
		if errors.Is(err, ErrAlreadyMigrated) {
			logging.Default().Info("KV migration already completed")
			return nil
		}
		return fmt.Errorf("validating version: %w", err)
	}
	if shouldDrop {
		// After unsuccessful migration attempt, clean KV table
		// Delete store if exists from previous failed KV migration and reopen store
		logging.Default().Warn("Removing KV table")
		// drop partitions first
		err = dropTables(ctx, dbPool, getTablePartitions(DefaultTableName, DefaultPartitions))
		if err != nil {
			return err
		}
		err = dropTables(ctx, dbPool, []string{DefaultTableName})
		if err != nil {
			return err
		}

		tmpStore, err := kv.Open(ctx, kvParams) // Open flow recreates table
		if err != nil {
			return fmt.Errorf("opening kv store: %w", err)
		}
		tmpStore.Close()
	}

	// Mark KV Migration started
	err = kv.SetDBSchemaVersion(ctx, store, 0)
	if err != nil {
		return err
	}

	// Import to KV Store
	var g multierror.Group
	var tables []string
	tmpDir, err := os.MkdirTemp("", "kv_migrate_")
	if err != nil {
		return fmt.Errorf("create temp dir: %w", err)
	}
	logger := logging.Default().WithField("TempDir", tmpDir)
	logger.Info("Starting KV Migration Process")
	logger.Warning("Make sure to follow the instructions in https://docs.lakefs.io/reference/upgrade.html (it's highly recommended to commit the data and take a DB snapshot)")
	defer timeTrack(time.Now(), logger, "KV Migration")
	for n, p := range kvPkgs {
		name := n
		migrateFunc := p.Func
		tables = append(tables, p.Tables...)
		g.Go(func() error {
			defer timeTrack(time.Now(), logger, fmt.Sprintf("%s Package Migration", name))
			fileLog := logging.Default().WithField("pkg_id", name)
			fileLog.Info("Starting KV migration for package")
			fd, err := os.CreateTemp(tmpDir, fmt.Sprintf("migrate_%s_", name))
			if err != nil {
				return fmt.Errorf("create temp file: %w", err)
			}
			defer fd.Close()
			err = migrateFunc(ctx, dbPool, fd)
			if err != nil {
				return fmt.Errorf("failed migration on package %s: %w", name, err)
			}
			_, err = fd.Seek(0, 0)
			if err != nil {
				return fmt.Errorf("failed seek file on package %s: %w", name, err)
			}
			err = kv.Import(ctx, fd, store)
			if err != nil {
				return fmt.Errorf("failed import on package %s: %w", name, err)
			}
			fileLog.Info("Successfully migrated package to KV")
			return nil
		})
	}
	err = g.Wait().ErrorOrNil()
	if err != nil {
		return err
	}

	// Update migrate version
	err = kv.SetDBSchemaVersion(ctx, store, kv.InitialMigrateVersion)
	if err != nil {
		return fmt.Errorf("failed setting migrate schema version: %w", err)
	}

	if dbParams.DropTables {
		err = dropTables(ctx, dbPool, tables)
		if err != nil {
			return err
		}
	}
	if err = os.RemoveAll(tmpDir); err != nil {
		logger.WithError(err).Warn("Failed to remove migration directory") // This should not fail the migration process
	}
	return nil
}

// getMigrationStatus Check KV version before migration. Version exists and smaller than InitialMigrateVersion indicates
// failed previous migration. In this case we drop the kv table and recreate it.
// In case version is equal or bigger - it means we already performed a successful migration and there's nothing to do
func getMigrationStatus(ctx context.Context, store kv.Store) (bool, error) {
	version, err := kv.GetDBSchemaVersion(ctx, store)
	if err != nil && errors.Is(err, kv.ErrNotFound) {
		return false, nil
	} else if err == nil { // Version exists in DB
		if version >= kv.InitialMigrateVersion {
			return false, ErrAlreadyMigrated
		}
		return true, nil
	}
	return false, err
}

func RegisterMigrate(name string, f MigrateFunc, tables []string) {
	kvPkgsMu.Lock()
	defer kvPkgsMu.Unlock()
	if _, ok := kvPkgs[name]; ok {
		panic(fmt.Sprintf("Package already registered: %s", name))
	}
	kvPkgs[name] = pkgMigrate{
		Func:   f,
		Tables: tables,
	}
}

// UnregisterAll remove all loaded migrate callbacks, used for test code.
func UnregisterAll() {
	kvPkgsMu.Lock()
	defer kvPkgsMu.Unlock()
	for k := range kvPkgs {
		delete(kvPkgs, k)
	}
}

func dropTables(ctx context.Context, dbPool *pgxpool.Pool, tables []string) error {
	if len(tables) < 1 {
		return nil
	}
	for i, table := range tables {
		tables[i] = pgx.Identifier{table}.Sanitize()
	}

	_, err := dbPool.Exec(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS %s CASCADE`, strings.Join(tables, ", ")))
	if err != nil {
		return fmt.Errorf("failed during drop tables: %s. Please perform manual cleanup of old database tables: %w", tables, err)
	}
	return nil
}
