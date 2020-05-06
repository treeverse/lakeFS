package db

import (
	"fmt"
	"net/url"

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
	schema, err := extractSchemaFromURI(uri)
	if err != nil {
		return nil, fmt.Errorf("could not open database: %w", err)
	}
	conn, err := sqlx.Connect(driver, uri)
	if err != nil {
		return nil, fmt.Errorf("could not open database: %w", err)
	}
	tx, err := conn.Beginx()
	if err != nil {
		return nil, fmt.Errorf("could not open database: %w", err)
	}
	_, err = tx.Exec("CREATE SCHEMA IF NOT EXISTS " + schema)
	if err != nil {
		return nil, fmt.Errorf("could not open database: %w", err)
	}
	err = tx.Commit()
	if err != nil {
		return nil, fmt.Errorf("could not open database: %w", err)
	}
	return NewDatabase(conn), nil
}
