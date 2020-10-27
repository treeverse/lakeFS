package db

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/golang-migrate/migrate/v4/source"

	"github.com/golang-migrate/migrate/v4"

	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/httpfs"
	"github.com/rakyll/statik/fs"
	"github.com/treeverse/lakefs/db/params"
	"github.com/treeverse/lakefs/ddl"
	"github.com/treeverse/lakefs/logging"
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
	err := MigrateUp(d.params)
	if err != nil {
		lg.WithError(err).Error("Failed to migrate")
		return err
	} else {
		lg.WithField("took", time.Since(start)).Info("schema migrated")
	}
	return nil
}

func getStatikSrc() (source.Driver, error) {
	// statik fs to our migrate source
	migrationFs, err := fs.NewWithNamespace(ddl.Ddl)
	if err != nil {
		return nil, err
	}
	return httpfs.New(migrationFs, "/")
}

func ValidateSchemaUpToDate(params params.Database) error {
	version, _, err := MigrateVersion(params)
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
	src, err := getStatikSrc()
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
	src, err := getStatikSrc()
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = src.Close()
	}()

	m, err := migrate.NewWithSourceInstance("httpfs", src, params.ConnectionString)
	if err != nil {
		return nil, err
	}
	return m, nil
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

func MigrateUp(p params.Database) error {
	m, err := getMigrate(p)
	if err != nil {
		return err
	}
	defer closeMigrate(m)
	err = m.Up()
	if err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return err
	}
	return nil
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

func MigrateTo(p params.Database, version uint, force bool) error {
	// make sure we have schema by calling connect
	mdb, err := ConnectDB(p)
	if err != nil {
		return err
	}
	defer func() {
		_ = mdb.Close()
	}()
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

func MigrateVersion(params params.Database) (uint, bool, error) {
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
