package api

import (
	"net/http"
	"strconv"
	"time"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/routers/legacy"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/treeverse/lakefs/pkg/httputil"
)

func MetricsMiddleware(swagger *openapi3.Swagger, requestHistogram *prometheus.HistogramVec, requestCounter *prometheus.CounterVec) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		// router for operation ID lookup
		router, err := legacy.NewRouter(swagger)
		if err != nil {
			panic(err)
		}

		// request histogram by operation ID
		requestHistogramHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			route, _, err := router.FindRoute(r)
			start := time.Now()
			mrw := httputil.NewMetricResponseWriter(w)
			next.ServeHTTP(mrw, r)
			if err == nil {
				requestHistogram.
					WithLabelValues(route.Operation.OperationID, strconv.Itoa(mrw.StatusCode)).
					Observe(time.Since(start).Seconds())
			}
		})

		// request handler
		return promhttp.InstrumentHandlerCounter(requestCounter, requestHistogramHandler)
	}
}
