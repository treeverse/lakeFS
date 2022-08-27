package dynamodb

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	dynamoRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "dynamo_request_duration_seconds",
		Help:    "Time spent doing DynamoDB requests.",
		Buckets: prometheus.ExponentialBuckets(0.001, 4, 9), // use 9 buckets from 1ms to just over 1 minute (65s).
	}, []string{"operation"})

	dynamoConsumedCapacity = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "dynamo_consumed_capacity_total",
		Help: "The capacity units consumed by operation.",
	}, []string{"operation"})

	dynamoFailures = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "dynamo_failures_total",
		Help: "The total number of errors while working for kv store.",
	}, []string{"operation"})
)
