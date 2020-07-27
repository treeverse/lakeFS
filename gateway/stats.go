package gateway

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var requestSummaries = promauto.NewSummaryVec(prometheus.SummaryOpts{Name: "gateway_requests_duration"}, []string{"operation_id", "code"})
