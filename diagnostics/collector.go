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

type Collector struct {
	db db.Database
}

const maxRecordsPerQueryCollect = 1000

var ErrNoColumns = errors.New("no columns in table")

func NewCollector(adb db.Database) *Collector {
	return &Collector{
		db: adb,
	}
}

func (c *Collector) Collect(ctx context.Context, w io.Writer) (err error) {
	writer := zip.NewWriter(w)
	defer func() { err = writer.Close() }()

	err = c.writeTableContent(ctx, writer, "auth_installation_metadata")
	if err != nil {
		return err
	}

	return nil
}

func (c *Collector) writeQueryContent(ctx context.Context, writer *zip.Writer, name string, selectBuilder sq.SelectBuilder) error {
	filename := name + ".csv"
	w, err := writer.Create(filename)
	if err != nil {
		return fmt.Errorf("new file for table %s - %w", name, err)
	}
	csvWriter := csv.NewWriter(w)
	defer csvWriter.Flush()

	q := selectBuilder.Limit(maxRecordsPerQueryCollect)
	query, args, err := q.ToSql()
	if err != nil {
		return fmt.Errorf("query build: %w", err)
	}
	rows, err := c.db.Query(query, args...)
	if err != nil {
		return fmt.Errorf("execute query: %w", err)
	}
	defer rows.Close()

	first := true
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		if first {
			first = false
			descriptions := rows.FieldDescriptions()
			if len(descriptions) == 0 {
				return ErrNoColumns
			}
			cols := make([]string, len(descriptions))
			for i, fd := range descriptions {
				cols[i] = string(fd.Name)
			}
			if err := csvWriter.Write(cols); err != nil {
				return fmt.Errorf("write csv header for %s %w", name, err)
			}
		}
		v, err := rows.Values()
		if err != nil {
			return err
		}
		record := make([]string, len(v))
		for i := range v {
			record[i] = fmt.Sprintf("%v", v[i])
		}
		if err := csvWriter.Write(record); err != nil {
			return err
		}
	}
	return rows.Err()
}

func (c *Collector) writeTableContent(ctx context.Context, writer *zip.Writer, name string) error {
	q := sq.Select("*").From(name)
	return c.writeQueryContent(ctx, writer, name, q)
}
