package db

import (
	"context"

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
		err := MigrateSchema(schema, url)
		if err != nil {
			log.WithError(err).WithField("url", url).Error("Failed to migrate")
			return err
		}
	}
	return nil
}

func MigrateSchema(schema, url string) error {
	// make sure we have schema by calling connect
	mdb, err := ConnectDB("pgx", url)
	if err != nil {
		return err
	}
	defer func() {
		_ = mdb.Close()
	}()

	// statik fs to our migrate source
	migrationFs, err := fs.NewWithNamespace(ddl.Ddl)
	if err != nil {
		return err
	}

	src, err := httpfs.New(migrationFs, "/"+schema+"/")
	if err != nil {
		return err
	}
	defer src.Close()

	m, err := migrate.NewWithSourceInstance("httpfs", src, url)
	if err != nil {
		return err
	}
	err = m.Up()
	if err != nil && err != migrate.ErrNoChange {
		return err
	}
	srcErr, dbErr := m.Close()
	if srcErr != nil {
		logging.Default().WithError(srcErr).Error("Migrate close source driver")
	}
	if dbErr != nil {
		logging.Default().WithError(dbErr).Error("Migrate close database connection")
	}
	return nil
}
