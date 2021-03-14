package api

import (
	"net/http"
	"strconv"
	"time"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/treeverse/lakefs/pkg/httputil"
)

func MetricsMiddleware(swagger *openapi3.Swagger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		router := openapi3filter.NewRouter().WithSwagger(swagger)

		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			route, _, err := router.FindRoute(r.Method, r.URL)
			start := time.Now()
			mrw := httputil.NewMetricResponseWriter(w)
			next.ServeHTTP(mrw, r)
			if err == nil {
				requestHistograms.
					WithLabelValues(route.Operation.OperationID, strconv.Itoa(mrw.StatusCode)).
					Observe(time.Since(start).Seconds())
			}
		})
	}
}
