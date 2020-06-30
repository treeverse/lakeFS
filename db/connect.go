package db

import (
	"fmt"
	"net/url"

	"github.com/treeverse/lakefs/logging"

	"github.com/jmoiron/sqlx"
)

func extractSchemaFromURI(uri string) (string, error) {
	var schema string
	parsed, err := url.Parse(uri)
	if err != nil {
		return schema, err
	}
	schema = parsed.Query().Get("search_path")
	if schema == "" {
		return "", fmt.Errorf("search_path not present in database connection string")
	}
	return schema, nil
}

func ConnectDB(driver string, uri string) (Database, error) {
	conn, err := sqlx.Connect(driver, uri)
	if err != nil {
		return nil, fmt.Errorf("could not open database: %w", err)
	}

	logging.Default().WithFields(logging.Fields{
		"driver": driver,
		"uri":    uri,
	}).Info("initialized DB connection")
	return NewSqlxDatabase(conn), nil
}
