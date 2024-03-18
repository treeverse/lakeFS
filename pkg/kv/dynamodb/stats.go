package dynamodb

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	dynamoConsumedCapacity = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "dynamo_consumed_capacity_total",
		Help: "The capacity units consumed by operation.",
	}, []string{"operation"})
	dynamoSlowdown = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "dynamo_slowdown_total",
		Help: "The number of times this operation was slowed down due to throttling.",
	}, []string{"operation"})
)
