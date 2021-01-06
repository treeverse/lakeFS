package diagnostics

import (
	"archive/zip"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"

	sq "github.com/Masterminds/squirrel"
	"github.com/treeverse/lakefs/db"
)

// DBCollector collects diagnostics information and write the collected content into a writer in a zip format
type DBCollector struct {
	db db.Database
}

const (
	maxRecordsPerQueryCollect = 1000
	csvFileExt                = ".csv"
)

var errNoColumnsFound = errors.New("no columns found")

// NewDBCollector accepts database to work with during collect
func NewDBCollector(adb db.Database) *DBCollector {
	return &DBCollector{
		db: adb,
	}
}

func (c *DBCollector) Collect(ctx context.Context, w io.Writer) (err error) {
	writer := zip.NewWriter(w)
	defer func() { err = writer.Close() }()

	errs := c.collectWithZip(ctx, writer)

	// write all errors into log
	if err := writeErrors(writer, errs); err != nil {
		return fmt.Errorf("write errors: %w", err)
	}

	return nil
}

// Collect query information from the database into csv files and write everything to io writer
func (c *DBCollector) collectWithZip(ctx context.Context, writer *zip.Writer) []error {
	var errs []error
	contentFromTables := []string{
		"auth_installation_metadata",
		"schema_migrations",
		"pg_stat_database",
	}
	for _, tbl := range contentFromTables {
		err := c.writeTableContent(ctx, writer, tbl)
		if err != nil {
			errs = append(errs, fmt.Errorf("write table content for %s: %w", tbl, err))
		}
	}

	countFromTables := []string{
		"catalog_branches",
		"catalog_commits",
		"catalog_repositories",
		"auth_users",
		"graveler_staging_kv",
		"graveler_repositories",
		"graveler_branches",
		"graveler_commits",
		"graveler_tags",
	}
	for _, tbl := range countFromTables {
		err := c.writeTableCount(ctx, writer, tbl)
		if err != nil {
			errs = append(errs, fmt.Errorf("write table count for %s: %w", tbl, err))
		}
	}

	err := c.writeRawQueryContent(ctx, writer, "db_version", `SELECT version();`)
	if err != nil {
		errs = append(errs, fmt.Errorf("get db version %w", err))
	}

	err = c.writeRawQueryContent(ctx, writer, "table_sizes", `
SELECT *, pg_size_pretty(total_bytes) AS total
    , pg_size_pretty(index_bytes) AS INDEX
    , pg_size_pretty(toast_bytes) AS toast
    , pg_size_pretty(table_bytes) AS TABLE
  FROM (SELECT *, total_bytes-index_bytes-COALESCE(toast_bytes,0) AS table_bytes FROM (
      SELECT c.oid,nspname AS table_schema, relname AS TABLE_NAME
              , c.reltuples AS row_estimate
              , pg_total_relation_size(c.oid) AS total_bytes
              , pg_indexes_size(c.oid) AS index_bytes
              , pg_total_relation_size(reltoastrelid) AS toast_bytes
          FROM pg_class c
          LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
          WHERE relkind = 'r') a) a`)
	if err != nil {
		errs = append(errs, fmt.Errorf("get table sizes %w", err))
	}

	return errs
}

func (c *DBCollector) writeQueryContent(ctx context.Context, writer *zip.Writer, name string, selectBuilder sq.SelectBuilder) error {
	q := selectBuilder.Limit(maxRecordsPerQueryCollect)
	query, args, err := q.ToSql()
	if err != nil {
		return fmt.Errorf("query build: %w", err)
	}
	return c.writeRawQueryContent(ctx, writer, name, query, args...)
}

func (c *DBCollector) writeRawQueryContent(ctx context.Context, writer *zip.Writer, name string, query string, args ...interface{}) error {
	_, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		rows, err := tx.Query(query, args...)
		if err != nil {
			return nil, fmt.Errorf("execute query: %w", err)
		}
		defer rows.Close()

		filename := name + csvFileExt
		w, err := writer.Create(filename)
		if err != nil {
			return nil, fmt.Errorf("new file for table %s - %w", name, err)
		}
		csvWriter := csv.NewWriter(w)
		defer csvWriter.Flush()

		first := true
		for rows.Next() {
			// write csv header row
			if first {
				first = false
				descriptions := rows.FieldDescriptions()
				if len(descriptions) == 0 {
					return nil, errNoColumnsFound
				}
				cols := make([]string, len(descriptions))
				for i, fd := range descriptions {
					cols[i] = string(fd.Name)
				}
				if err := csvWriter.Write(cols); err != nil {
					return nil, fmt.Errorf("write csv header for %s: %w", name, err)
				}
			}

			// format and write data row
			v, err := rows.Values()
			if err != nil {
				return nil, err
			}
			record := make([]string, len(v))
			for i := range v {
				record[i] = fmt.Sprintf("%v", v[i])
			}
			if err := csvWriter.Write(record); err != nil {
				return nil, err
			}
		}
		return nil, rows.Err()
	}, db.WithContext(ctx), db.ReadOnly())
	return err
}

func (c *DBCollector) writeTableContent(ctx context.Context, writer *zip.Writer, name string) error {
	q := sq.Select("*").From(name)
	return c.writeQueryContent(ctx, writer, name, q)
}

func (c *DBCollector) writeTableCount(ctx context.Context, writer *zip.Writer, name string) error {
	q := sq.Select("COUNT(*)").From(name)
	return c.writeQueryContent(ctx, writer, name+"_count", q)
}

func writeErrors(writer *zip.Writer, errs []error) error {
	if len(errs) == 0 {
		return nil
	}
	w, err := writer.Create("errors.log")
	if err != nil {
		return err
	}
	for _, err := range errs {
		_, _ = fmt.Fprintf(w, "%v\n", err)
	}
	return nil
}
