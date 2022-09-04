package db

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source"
	"github.com/golang-migrate/migrate/v4/source/httpfs"
	blockparams "github.com/treeverse/lakefs/pkg/block/params"
	"github.com/treeverse/lakefs/pkg/db/params"
	"github.com/treeverse/lakefs/pkg/ddl"
	_ "github.com/treeverse/lakefs/pkg/kv/dynamodb"
	kvparams "github.com/treeverse/lakefs/pkg/kv/params"
	kvpg "github.com/treeverse/lakefs/pkg/kv/postgres"
	"github.com/treeverse/lakefs/pkg/logging"
	"gopkg.in/retry.v1"
)

var ErrSchemaNotCompatible = errors.New("db schema version not compatible with latest version")

type Migrator interface {
	Migrate(ctx context.Context) error
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

func MigrateUp(dbParams params.Database, blockParams blockparams.AdapterConfig, kvParams kvparams.KV) error {
	m, err := getMigrate(dbParams)
	if err != nil {
		return err
	}
	defer closeMigrate(m)
	err = m.Up()
	if err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return err
	}
	ctx := context.Background()
	d := BuildDatabaseConnection(ctx, dbParams)
	defer d.Close()
	return kvpg.Migrate(ctx, d.Pool(), dbParams, blockParams, kvParams.Postgres)
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
