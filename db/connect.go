package db

import (
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/treeverse/lakefs/logging"
)

func ConnectDB(driver string, uri string) (Database, error) {
	conn, err := sqlx.Connect(driver, uri)
	if err != nil {
		return nil, fmt.Errorf("could not open database: %w", err)
	}

	conn.SetMaxOpenConns(25)
	conn.SetMaxIdleConns(25)
	conn.SetConnMaxLifetime(5 * time.Minute)

	logging.Default().WithFields(logging.Fields{
		"driver": driver,
		"uri":    uri,
	}).Info("initialized DB connection")
	return NewSqlxDatabase(conn), nil
}
