package db

import (
	"context"
	"errors"
	"os"
	"time"

	"github.com/golang-migrate/migrate/v4/source"

	"github.com/golang-migrate/migrate/v4"

	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/httpfs"
	"github.com/rakyll/statik/fs"
	"github.com/treeverse/lakefs/ddl"
	"github.com/treeverse/lakefs/logging"
)

type Migrator interface {
	Migrate(ctx context.Context) error
}

type DatabaseMigrator struct {
	connectionString string
}

func NewDatabaseMigrator(connectionString string) *DatabaseMigrator {
	return &DatabaseMigrator{
		connectionString: connectionString,
	}
}

func (d *DatabaseMigrator) Migrate(ctx context.Context) error {
	log := logging.FromContext(ctx)
	start := time.Now()
	lg := log.WithFields(logging.Fields{
		"direction": "up",
	})
	err := MigrateUp(d.connectionString)
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

func GetLastMigrationAvailable(from uint) (uint, error) {
	src, err := getStatikSrc()
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = src.Close()
	}()
	current := from
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

func getMigrate(connectionString string) (*migrate.Migrate, error) {
	src, err := getStatikSrc()
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = src.Close()
	}()

	m, err := migrate.NewWithSourceInstance("httpfs", src, connectionString)
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

func MigrateUp(connectionString string) error {
	//make sure we have schema by calling connect
	mdb, err := ConnectDB("pgx", connectionString)
	if err != nil {
		return err
	}
	defer func() {
		_ = mdb.Close()
	}()
	m, err := getMigrate(connectionString)
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

func MigrateDown(connectionString string) error {
	m, err := getMigrate(connectionString)
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

func MigrateTo(connectionString string, version uint) error {
	//make sure we have schema by calling connect
	mdb, err := ConnectDB("pgx", connectionString)
	if err != nil {
		return err
	}
	defer func() {
		_ = mdb.Close()
	}()
	m, err := getMigrate(connectionString)
	if err != nil {
		return err
	}
	defer closeMigrate(m)
	err = m.Migrate(version)
	if err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return err
	}
	return nil
}

func MigrateVersion(connectionString string) (uint, bool, error) {
	m, err := getMigrate(connectionString)
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
