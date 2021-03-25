package db

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	dbRetriesCount = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "db_retries",
			Help: "A database retries counter",
		},
	)

	dbErrorsCounter = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "db_errors",
			Help: "A database errors counter",
		},
		[]string{"op"},
	)
)
