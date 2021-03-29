package api

import (
	"net/http"
	"strconv"
	"time"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/routers/legacy"
	"github.com/treeverse/lakefs/pkg/httputil"
)

func MetricsMiddleware(swagger *openapi3.Swagger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		router, err := legacy.NewRouter(swagger)
		if err != nil {
			panic(err)
		}
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	}
}
