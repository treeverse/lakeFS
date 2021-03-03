package diagnostics

import (
	"archive/zip"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"strconv"

	sq "github.com/Masterminds/squirrel"
	"github.com/hashicorp/go-multierror"
	pyramidparams "github.com/treeverse/lakefs/pyramid/params"

	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/db"
)

// Collector collects diagnostics information and write the collected content into a writer in a zip format
type Collector struct {
	cataloger  catalog.Cataloger
	adapter    block.Adapter
	pyramidCfg *pyramidparams.ExtParams
	db         db.Database
}

// NewCollector accepts database to work with during collect
func NewCollector(adb db.Database, cataloger catalog.Cataloger, cfg *pyramidparams.ExtParams, adapter block.Adapter) *Collector {
	return &Collector{
		cataloger:  cataloger,
		pyramidCfg: cfg,
		adapter:    adapter,
		db:         adb,
	}
}

const (
	maxRecordsPerQueryCollect = 1000
	csvFileExt                = ".csv"
)

var errNoColumnsFound = errors.New("no columns found")

// Collect query information from the database into csv files and write everything to io writer
func (c *Collector) Collect(ctx context.Context, w io.Writer) error {
	writer := zip.NewWriter(w)

	combinedErr := c.collectDB(ctx, writer)

	if err := c.rangesStats(ctx, writer); err != nil {
		combinedErr = multierror.Append(combinedErr, err)
	}
	if err := writeErrors(writer, combinedErr); err != nil {
		combinedErr = multierror.Append(combinedErr, fmt.Errorf("write errors: %w", err))
	}

	if err := writer.Close(); err != nil {
		combinedErr = multierror.Append(combinedErr, err)
	}
	return combinedErr
}

// collectDB query information from the database into csv files and write everything to io writer
func (c *Collector) collectDB(ctx context.Context, writer *zip.Writer) error {
	var combinedErr error
	contentFromTables := []string{
		"auth_installation_metadata",
		"schema_migrations",
		"pg_stat_database",
	}
	for _, tbl := range contentFromTables {
		if err := c.writeTableContent(ctx, writer, tbl); err != nil {
			combinedErr = multierror.Append(combinedErr, fmt.Errorf("write table content for %s: %w", tbl, err))
		}
	}

	countFromTables := []string{
		"auth_users",
		"graveler_staging_kv",
		"graveler_repositories",
		"graveler_branches",
		"graveler_commits",
		"graveler_tags",
	}
	for _, tbl := range countFromTables {
		if err := c.writeTableCount(ctx, writer, tbl); err != nil {
			combinedErr = multierror.Append(combinedErr, fmt.Errorf("write table count for %s: %w", tbl, err))
		}
	}

	if err := c.writeRawQueryContent(ctx, writer, "db_version", `SELECT version();`); err != nil {
		combinedErr = multierror.Append(combinedErr, fmt.Errorf("get db version %w", err))
	}

	if err := c.writeRawQueryContent(ctx, writer, "table_sizes", `
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
          WHERE relkind = 'r') a) a`); err != nil {
		combinedErr = multierror.Append(combinedErr, fmt.Errorf("get table sizes %w", err))
	}

	return combinedErr
}

func (c *Collector) writeTableContent(ctx context.Context, writer *zip.Writer, name string) error {
	q := sq.Select("*").From(name)
	return c.writeQueryContent(ctx, writer, name, q)
}

func (c *Collector) writeQueryContent(ctx context.Context, writer *zip.Writer, name string, selectBuilder sq.SelectBuilder) error {
	q := selectBuilder.Limit(maxRecordsPerQueryCollect)
	query, args, err := q.ToSql()
	if err != nil {
		return fmt.Errorf("query build: %w", err)
	}
	return c.writeRawQueryContent(ctx, writer, name, query, args...)
}

func (c *Collector) writeRawQueryContent(ctx context.Context, writer *zip.Writer, name string, query string, args ...interface{}) error {
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

func (c *Collector) writeTableCount(ctx context.Context, writer *zip.Writer, name string) error {
	q := sq.Select("COUNT(*)").From(name)
	return c.writeQueryContent(ctx, writer, name+"_count", q)
}

func (c *Collector) rangesStats(ctx context.Context, writer *zip.Writer) error {
	var combinedErr error
	repos, _, err := c.cataloger.ListRepositories(ctx, -1, "")
	if err != nil {
		// Cannot list repos, nothing to do..
		combinedErr = multierror.Append(combinedErr, fmt.Errorf("listing repositories: %w", err))
		return combinedErr
	}

	rangesFile, err := writer.Create("graveler-ranges")
	if err != nil {
		combinedErr = multierror.Append(combinedErr, fmt.Errorf("creating ranges file: %w", err))
		return combinedErr
	}

	csvWriter := csv.NewWriter(rangesFile)
	defer csvWriter.Flush()
	if err := csvWriter.Write([]string{"repo", "type", "count(max:1000)"}); err != nil {
		combinedErr = multierror.Append(combinedErr, fmt.Errorf("writing headers: %w", err))
	}

	for _, repo := range repos {
		count := 0
		counter := func(id string) error {
			count++
			return nil
		}

		if err := c.adapter.Walk(ctx, block.WalkOpts{
			StorageNamespace: repo.StorageNamespace,
			Prefix:           c.pyramidCfg.BlockStoragePrefix,
		}, counter); err != nil {
			combinedErr = multierror.Append(combinedErr, fmt.Errorf("listing ranges and meta-ranges: %w", err))
		}
		if err := csvWriter.Write([]string{repo.Name, "range", strconv.Itoa(count)}); err != nil {
			combinedErr = multierror.Append(combinedErr, fmt.Errorf("writing meta-ranges for repo (%s): %w", repo.Name, err))
		}
	}

	return combinedErr
}

func writeErrors(writer *zip.Writer, err error) error {
	if err == nil {
		return nil
	}

	w, e := writer.Create("errors.log")
	if e != nil {
		return e
	}

	_, e = fmt.Fprint(w, err.Error())
	return e
}
