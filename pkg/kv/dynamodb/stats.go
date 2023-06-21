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
)
