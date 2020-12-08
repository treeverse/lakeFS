package pyramid

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// nolint: gomnd
const (
	kb                = float64(1024)
	mb                = 1024 * kb
	fsNameLabel       = "fsName"
	errorTypeLabel    = "type"
	accessStatusLabel = "status"
)

var cacheAccess = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "tier_fs_cache_hits_total",
		Help: "TierFS cache hits total count",
	}, []string{fsNameLabel, accessStatusLabel})

var errorsTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "tier_fs_errors_total",
		Help: "TierFS errors by type",
	}, []string{fsNameLabel, errorTypeLabel})

var evictionHistograms = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "tier_fs_eviction_bytes",
		Help:    "TierFS evicted object size by bytes",
		Buckets: []float64{0.5 * kb, 1 * kb, 16 * kb, 32 * kb, 128 * kb, 512 * kb, 1 * mb, 2 * mb, 4 * mb, 8 * mb, 16 * mb, 64 * mb},
	},
	[]string{fsNameLabel})

var downloadHistograms = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "tier_fs_download_bytes",
		Help:    "TierFS download from block-store object size by bytes",
		Buckets: []float64{0.5 * kb, 1 * kb, 16 * kb, 32 * kb, 128 * kb, 512 * kb, 1 * mb, 2 * mb, 4 * mb, 8 * mb, 16 * mb, 64 * mb},
	},
	[]string{fsNameLabel})
