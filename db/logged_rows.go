package db

import (
	"github.com/treeverse/lakefs/logging"

	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgx/v4"
)

// LoggedRows is a pgx.Rows that wraps and traces another pgx.Rows.
type LoggedRows struct {
	pgx.Rows
	Start  time.Time
	Closed bool
	l      logging.Logger
}

var _ pgx.Rows = &LoggedRows{}

// Logged returns a pgx.Rows that will forward calls to r and logs their durations.
func Logged(r pgx.Rows, start time.Time, l logging.Logger) *LoggedRows {
	return &LoggedRows{
		Rows:  r,
		Start: start,
		l:     l,
	}
}

func (lr *LoggedRows) logDuration() {
	if !lr.Closed {
		lr.l.WithField("duration", time.Since(lr.Start)).Info("rows done")
		lr.Closed = true
	}
}

func (lr *LoggedRows) Close() {
	lr.Rows.Close()
	lr.logDuration()
}

func (lr *LoggedRows) Next() bool {
	ret := lr.Rows.Next()
	if !ret {
		// Underlying Rows closed itself, lr.Close() was not called
		lr.logDuration()
	}
	return ret
}

func (lr *LoggedRows) Err() error {
	return lr.Rows.Err()
}

func (lr *LoggedRows) CommandTag() pgconn.CommandTag {
	return lr.Rows.CommandTag()
}

func (lr *LoggedRows) FieldDescriptions() []pgproto3.FieldDescription {
	return lr.Rows.FieldDescriptions()
}

func (lr *LoggedRows) Scan(dest ...interface{}) error {
	return lr.Rows.Scan(dest...)
}

func (lr *LoggedRows) Values() ([]interface{}, error) {
	return lr.Rows.Values()
}

func (lr *LoggedRows) RawValues() [][]byte {
	return lr.Rows.RawValues()
}
