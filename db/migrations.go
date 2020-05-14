package db

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/rakyll/statik/fs"
	"github.com/treeverse/lakefs/ddl"
	"github.com/treeverse/lakefs/logging"
)

const schemaTableDDL = `
CREATE TABLE IF NOT EXISTS schema_versions (
    id serial NOT NULL PRIMARY KEY,
    version varchar(128) NOT NULL,
    migrated_at timestamptz NOT NULL
);`

const insertVersionSQL = `
INSERT INTO schema_versions (version, migrated_at)
VALUES ($1, $2)
`

type Migrator interface {
	Migrate(ctx context.Context) error
}

type DatabaseMigrator struct {
	databases map[string]Database
}

func NewDatabaseMigrator() *DatabaseMigrator {
	return &DatabaseMigrator{
		databases: make(map[string]Database),
	}
}

func (d *DatabaseMigrator) AddDB(schema string, db Database) *DatabaseMigrator {
	d.databases[schema] = db
	return d
}

func (d *DatabaseMigrator) Migrate(ctx context.Context) error {
	for schema, db := range d.databases {
		_, err := db.Transact(func(tx Tx) (interface{}, error) {
			return nil, MigrateSchemaAll(tx, schema)
		}, WithLogger(logging.FromContext(ctx)))
		if err != nil {
			return err
		}
	}
	return nil
}

func GetDDL(schemaName, version string) (string, error) {
	migrationFs, err := fs.NewWithNamespace(ddl.Ddl)
	if err != nil {
		return "", err
	}

	filePath := fmt.Sprintf("/%s/%s.sql", schemaName, version)
	reader, err := migrationFs.Open(filePath)
	if err != nil {
		return "", fmt.Errorf("could not open file %s: %v", filePath, err)
	}
	defer func() {
		_ = reader.Close()
	}()
	contents, err := ioutil.ReadAll(reader)
	if err != nil {
		return "", fmt.Errorf("could not read file %s: %v", filePath, err)
	}
	return string(contents), nil
}

func ListSchemas() ([]string, error) {
	migrationFs, err := fs.NewWithNamespace(ddl.Ddl)
	if err != nil {
		return nil, err
	}
	schemas := make([]string, 0)
	err = fs.Walk(migrationFs, "/", func(pth string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() && !strings.EqualFold(pth, "/") {
			schemas = append(schemas, strings.Trim(pth, "/"))
		}
		return nil
	})
	sort.Strings(schemas)
	return schemas, err
}

func ListVersions(schemaName string) ([]string, error) {
	migrationFs, err := fs.NewWithNamespace(ddl.Ddl)
	if err != nil {
		return nil, err
	}
	versions := make([]string, 0)
	baseDir := fmt.Sprintf("/%s", schemaName)
	err = fs.Walk(migrationFs, baseDir, func(pth string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && strings.HasSuffix(pth, ".sql") {
			versions = append(versions, strings.TrimSuffix(path.Base(pth), ".sql"))
		}
		return nil
	})
	sort.Strings(versions)
	return versions, err
}

func MigrateSchemaAll(tx Tx, schemaName string) error {
	// make sure we have schema
	_, err := tx.Exec("CREATE SCHEMA IF NOT EXISTS " + schemaName)
	if err != nil {
		return fmt.Errorf("failed to create schema '%s' if not exists: %w", schemaName, err)
	}
	versions, err := ListVersions(schemaName)
	if err != nil {
		return err
	}
	for _, version := range versions {
		err = MigrateVersion(tx, schemaName, version)
		if err != nil {
			return err
		}
	}
	return nil
}

func MigrateVersion(tx Tx, schemaName, version string) error {
	ddlCommands, err := GetDDL(schemaName, version)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ddlCommands)
	if err != nil {
		return err
	}
	_, err = tx.Exec(schemaTableDDL)
	if err != nil {
		return err
	}
	_, err = tx.Exec(insertVersionSQL, version, time.Now())
	return err
}
