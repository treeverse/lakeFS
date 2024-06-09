package auth

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	authDurationSecs = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "auth_request_duration_seconds",
			Help:    "request durations for auth",
			Buckets: []float64{0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1, 2, 5, 10, 30, 60},
		}, []string{"operation", "success"})
)

func NewMonitoredAuthService(service Service) *MonitoredService {
	return &MonitoredService{
		Wrapped: service,
		Observe: func(op string, duration time.Duration, success bool) {
			status := "failure"
			if success {
				status = "success"
			}
			authDurationSecs.WithLabelValues(op, status).Observe(duration.Seconds())
		},
	}
}
