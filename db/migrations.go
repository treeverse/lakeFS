package db

import (
	"context"
	"errors"
	"os"

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
	databases map[string]string
}

func NewDatabaseMigrator() *DatabaseMigrator {
	return &DatabaseMigrator{
		databases: make(map[string]string),
	}
}

func (d *DatabaseMigrator) AddDB(schema string, url string) *DatabaseMigrator {
	d.databases[schema] = url
	return d
}

func (d *DatabaseMigrator) Migrate(ctx context.Context) error {
	log := logging.FromContext(ctx)
	for schema, url := range d.databases {
		err := MigrateUp(schema, url)
		if err != nil {
			log.WithError(err).WithField("url", url).Error("Failed to migrate")
			return err
		}
	}
	return nil
}

func getStatikSrc(schema string) (source.Driver, error) {
	// statik fs to our migrate source
	migrationFs, err := fs.NewWithNamespace(ddl.Ddl)
	if err != nil {
		return nil, err
	}
	return httpfs.New(migrationFs, "/"+schema+"/")
}

func GetLastMigrationAvailable(schema string, from uint) (uint, error) {
	src, err := getStatikSrc(schema)
	if err != nil {
		return 0, err
	}
	defer src.Close()
	current := from
	for {
		next, err := src.Next(current)
		if errors.Is(err, os.ErrNotExist) {
			return current, nil
		} else if err != nil {
			return 0, err
		}
		current = next
	}
}

func getMigrate(schema string, url string) (*migrate.Migrate, error) {
	src, err := getStatikSrc(schema)
	if err != nil {
		return nil, err
	}
	defer src.Close()

	m, err := migrate.NewWithSourceInstance("httpfs", src, url)
	if err != nil {
		return nil, err
	}
	return m, nil
}

func closeMigrate(m *migrate.Migrate) {
	srcErr, dbErr := m.Close()
	if srcErr != nil {
		logging.Default().WithError(srcErr).Error("Migrate close source driver")
	}
	if dbErr != nil {
		logging.Default().WithError(dbErr).Error("Migrate close database connection")
	}
}

func MigrateUp(schema, url string) error {
	//make sure we have schema by calling connect
	mdb, err := ConnectDB("pgx", url)
	if err != nil {
		return err
	}
	defer func() {
		_ = mdb.Close()
	}()
	m, err := getMigrate(schema, url)
	if err != nil {
		return err
	}
	defer closeMigrate(m)
	err = m.Up()
	if err != nil && err != migrate.ErrNoChange {
		return err
	}
	return nil
}

func MigrateDown(schema, url string) error {
	m, err := getMigrate(schema, url)
	if err != nil {
		return err
	}
	defer closeMigrate(m)
	err = m.Down()
	if err != nil && err != migrate.ErrNoChange {
		return err
	}
	return nil
}

func MigrateTo(schema, url string, version uint) error {
	//make sure we have schema by calling connect
	mdb, err := ConnectDB("pgx", url)
	if err != nil {
		return err
	}
	defer func() {
		_ = mdb.Close()
	}()
	m, err := getMigrate(schema, url)
	if err != nil {
		return err
	}
	defer closeMigrate(m)
	err = m.Migrate(version)
	if err != nil && err != migrate.ErrNoChange {
		return err
	}
	return nil
}

func MigrateVersion(schema, url string) (uint, bool, error) {
	m, err := getMigrate(schema, url)
	if err != nil {
		return 0, false, err
	}
	defer closeMigrate(m)
	version, dirty, err := m.Version()
	if err != nil && err != migrate.ErrNoChange {
		return 0, false, err
	}
	return version, dirty, err
}
