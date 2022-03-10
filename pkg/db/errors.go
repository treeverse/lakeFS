package db

import (
	"errors"
	"fmt"
	"net"

	"github.com/georgysavva/scany/pgxscan"
	"github.com/jackc/pgx/v4"
)

var (
	ErrNotFound      = fmt.Errorf("not found: %w", pgx.ErrNoRows)
	ErrAlreadyExists = errors.New("already exists")
	ErrSerialization = errors.New("serialization error")
)

func isDialError(err error) bool {
	netError := &net.OpError{}
	return errors.As(err, &netError) && netError.Op == "dial"
}

func (d *dbTx) handleSQLError(err error, cmdType string, query string) error {
	dbErrorsCounter.WithLabelValues(cmdType).Inc()
	d.logger.WithError(err).Error("SQL query failed with error")

	// Each error that is added here should be updated also in controller.go:handleAPIError
	if isUniqueViolation(err) {
		return ErrAlreadyExists
	}
	if pgxscan.NotFound(err) || errors.Is(err, pgx.ErrNoRows) {
		d.logger.Trace("SQL query returned no results")
		return ErrNotFound
	}
	return fmt.Errorf("query %s: %w", query, err)
}
