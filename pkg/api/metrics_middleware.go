package api

import (
	"net/http"
	"strconv"
	"time"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/routers/gorillamux"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/treeverse/lakefs/pkg/httputil"
)

func MetricsMiddleware(swagger *openapi3.T) func(http.Handler) http.Handler {
	// router for operation ID lookup
	router, err := gorillamux.NewRouter(swagger)
	if err != nil {
		panic(err)
	}
	return func(next http.Handler) http.Handler {
		// request histogram by operation ID
		requestHistogramHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			route, _, err := router.FindRoute(r)
			start := time.Now()
			mrw := httputil.NewMetricResponseWriter(w)
			next.ServeHTTP(mrw, r)
			if err == nil {
				requestHistograms.
					WithLabelValues(route.Operation.OperationID, strconv.Itoa(mrw.StatusCode)).
					Observe(time.Since(start).Seconds())
			}
		})

		// request handler
		return promhttp.InstrumentHandlerCounter(requestCounter, requestHistogramHandler)
	}
}
