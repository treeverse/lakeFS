package db

import (
	"fmt"
	"time"

	"github.com/dlmiddlecote/sqlstats"
	"github.com/jmoiron/sqlx"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/treeverse/lakefs/logging"
)

func ConnectDB(driver string, uri string) (Database, error) {
	log := logging.Default().WithFields(logging.Fields{
		"driver": driver,
		"uri":    uri,
	})

	log.Info("connecting to the DB")
	conn, err := sqlx.Connect(driver, uri)
	if err != nil {
		return nil, fmt.Errorf("could not open DB: %w", err)
	}

	conn.SetMaxOpenConns(25)
	conn.SetMaxIdleConns(25)
	conn.SetConnMaxLifetime(5 * time.Minute)

	log.Info("initialized DB connection")
	registerPrometheusCollector(conn)
	return NewSqlxDatabase(conn), nil
}

func registerPrometheusCollector(conn *sqlx.DB) {
	collector := sqlstats.NewStatsCollector("lakefs", conn)
	err := prometheus.Register(collector)
	if err != nil {
		logging.Default().WithError(err).Error("failed to register db stats collector")
	}
}
