package db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source"
	"github.com/golang-migrate/migrate/v4/source/httpfs"
	"github.com/hashicorp/go-multierror"
	"github.com/treeverse/lakefs/pkg/db/params"
	"github.com/treeverse/lakefs/pkg/ddl"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/export"
	kvpg "github.com/treeverse/lakefs/pkg/kv/postgres"
	"github.com/treeverse/lakefs/pkg/logging"
	"gopkg.in/retry.v1"
)

var ErrSchemaNotCompatible = errors.New("db schema version not compatible with latest version")

type Migrator interface {
	Migrate(ctx context.Context) error
}

type DatabaseMigrator struct {
	params params.Database
}

func NewDatabaseMigrator(params params.Database) *DatabaseMigrator {
	return &DatabaseMigrator{
		params: params,
	}
}

func (d *DatabaseMigrator) Migrate(ctx context.Context) error {
	log := logging.FromContext(ctx)
	start := time.Now()
	lg := log.WithFields(logging.Fields{
		"direction": "up",
	})
	err := MigrateUp(d.params, false)
	if err != nil {
		lg.WithError(err).Error("Failed to migrate")
		return err
	} else {
		lg.WithField("took", time.Since(start)).Info("schema migrated")
	}
	return nil
}

// newDriverSourceForDDLContent migration driver source with our DDL content
func newDriverSourceForDDLContent() (source.Driver, error) {
	return httpfs.New(http.FS(ddl.Content), "/")
}

func ValidateSchemaUpToDate(ctx context.Context, dbPool Database, params params.Database) error {
	version, _, err := MigrateVersion(ctx, dbPool, params)
	if err != nil {
		return err
	}
	available, err := GetLastMigrationAvailable()
	if err != nil {
		return err
	}
	if available != version {
		return fmt.Errorf("%w: db version=%d, available=%d", ErrSchemaNotCompatible, version, available)
	}
	return nil
}

func GetLastMigrationAvailable() (uint, error) {
	src, err := newDriverSourceForDDLContent()
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = src.Close()
	}()
	current, err := src.First()
	if err != nil {
		return 0, fmt.Errorf("%w: failed to find first migration", err)
	}
	for {
		next, err := src.Next(current)
		if errors.Is(err, os.ErrNotExist) {
			return current, nil
		}
		if err != nil {
			return 0, err
		}
		current = next
	}
}

func getMigrate(params params.Database) (*migrate.Migrate, error) {
	src, err := newDriverSourceForDDLContent()
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = src.Close()
	}()
	connectionString := params.ConnectionString
	m, err := tryNewWithSourceInstance("httpfs", src, connectionString)
	if err != nil {
		return nil, err
	}
	return m, nil
}

// getMigrateDatabaseDriver returns a database name and a (migrate)
// database.Driver for use in migration.  Use it to prevent
// migrate.NewWithSourceInstance from applying defaults when databaseURL is
// empty -- those defaults will be different from the ones in pg/pgx.
func getMigrateDatabaseDriver(databaseURL string) (string, database.Driver, error) {
	driver, err := (&postgres.Postgres{}).Open(databaseURL)
	return "postgres", driver, err
}

func tryNewWithSourceInstance(sourceName string, sourceInstance source.Driver, databaseURL string) (*migrate.Migrate, error) {
	strategy := params.DatabaseRetryStrategy
	var m *migrate.Migrate
	var err error
	for a := retry.Start(strategy, nil); a.Next(); {
		var (
			dbName   string
			dbDriver database.Driver
		)
		dbName, dbDriver, err = getMigrateDatabaseDriver(databaseURL)
		if err != nil {
			return nil, fmt.Errorf("get migrate database driver: %w", err)
		}
		m, err = migrate.NewWithInstance(sourceName, sourceInstance, dbName, dbDriver)
		if err == nil {
			return m, nil
		}
		if !isDialError(err) {
			return nil, fmt.Errorf("error while connecting to DB: %w", err)
		}
		if a.More() {
			logging.Default().WithError(err).Info("Could not connect to DB: Trying again")
		}
	}

	return nil, fmt.Errorf("retries exhausted, could not connect to DB: %w", err)
}

func closeMigrate(m *migrate.Migrate) {
	srcErr, dbErr := m.Close()
	if srcErr != nil {
		logging.Default().WithError(srcErr).Error("failed to close source driver")
	}
	if dbErr != nil {
		logging.Default().WithError(dbErr).Error("failed to close database connection")
	}
}

func MigrateUp(p params.Database, dropTables bool) error {
	m, err := getMigrate(p)
	if err != nil {
		return err
	}
	defer closeMigrate(m)
	err = m.Up()
	if err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return err
	}
	return kvMigrate(p, m, dropTables)
}

func MigrateDown(params params.Database) error {
	m, err := getMigrate(params)
	if err != nil {
		return err
	}
	defer closeMigrate(m)
	err = m.Down()
	if err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return err
	}
	return nil
}

func MigrateTo(ctx context.Context, p params.Database, version uint, force bool) error {
	// make sure we have schema by calling connect
	mdb, err := ConnectDB(ctx, p)
	if err != nil {
		return err
	}
	defer mdb.Close()
	m, err := getMigrate(p)
	if err != nil {
		return err
	}
	defer closeMigrate(m)
	if force {
		err = m.Force(int(version))
	} else {
		err = m.Migrate(version)
	}
	if err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return err
	}
	return nil
}

func MigrateVersion(ctx context.Context, dbPool Database, params params.Database) (uint, bool, error) {
	// validate that default migrations table exists with information - a workaround
	// so we will not create the migration table as the package will ensure the table exists
	var rows int
	err := dbPool.Get(ctx, &rows, `SELECT COUNT(*) FROM `+postgres.DefaultMigrationsTable)
	if err != nil || rows == 0 {
		return 0, false, migrate.ErrNilVersion
	}

	// get version from migrate
	m, err := getMigrate(params)
	if err != nil {
		return 0, false, err
	}
	defer closeMigrate(m)
	version, dirty, err := m.Version()
	if err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return 0, false, err
	}
	return version, dirty, err
}

// TODO: (niro) Remove on feature complete

// kvMigrate implementation of KV Migration logic
func kvMigrate(dbParams params.Database, m *migrate.Migrate, drop bool) error {
	if !dbParams.KVEnabled {
		return nil
	}
	ctx := context.Background()
	dbPool := BuildDatabaseConnection(ctx, dbParams)
	defer dbPool.Close()
	version, dirty, err := MigrateVersion(ctx, dbPool, dbParams)
	if err != nil {
		return err
	}
	if dirty {
		return migrate.ErrDirty{Version: int(version)}
	}
	if kv.MigrateVersion != version {
		return migrate.ErrInvalidVersion
	}

	// Delete store if exists from previous failed KV migration
	err = dropTables(ctx, dbPool, []string{kvpg.DefaultTableName})
	if err != nil {
		return err
	}

	kvStore, err := kv.Open(ctx, kvpg.DriverName, dbParams.ConnectionString)
	if err != nil {
		return err
	}

	// Import to KV Store
	var g multierror.Group
	for n, f := range kvPkgs {
		name := n
		migrateFunc := f
		g.Go(func() error {
			logging.Default().Info("Starting KV migration for package:", name)
			buf := bytes.Buffer{}
			err := migrateFunc(dbPool, &buf)
			if err != nil {
				logging.Default().WithError(err).Error("Failed migration on pkg:", name)
				return err
			}
			err = export.ImportFile(ctx, &buf, kvStore)
			if err != nil {
				logging.Default().WithError(err).Error("Failed export on pkg:", name)
				return err
			}
			logging.Default().Info("Successfully migrated pkg", name, "to KV")
			return nil
		})
	}
	err = g.Wait().ErrorOrNil()
	if err != nil {
		return err
	}

	// Update migrate version
	err = m.Force(kv.MigrateVersion + 1)
	if err != nil {
		logging.Default().WithError(err).Error("Failed setting migrate version")
		return err
	}

	if drop {
		tables := []string{"gateway_multiparts"}
		err = dropTables(ctx, dbPool, tables)
	}
	return err
}

type MigrateFunc func(db Database, writer io.Writer) error

var (
	kvPkgs     = make(map[string]MigrateFunc)
	registerMu sync.RWMutex
)

func KVRegister(name string, f MigrateFunc) {
	registerMu.Lock()
	defer registerMu.Unlock()
	kvPkgs[name] = f
}

// KVUnRegisterAll remove all loaded migrate callbacks, used for test code.
func KVUnRegisterAll() {
	registerMu.Lock()
	defer registerMu.Unlock()
	for k := range kvPkgs {
		delete(kvPkgs, k)
	}
}

// TODO(niro): For each pkg implementation add relevant tables
func dropTables(ctx context.Context, db Database, tables []string) error {
	_, err := db.Exec(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS %s`, strings.Join(tables, ", ")))
	if err != nil {
		logging.Default().WithError(err).Error(
			"Failed during drop tables:", tables, ". Please perform manual cleanup of old database tables")
		return err
	}
	return nil
}
