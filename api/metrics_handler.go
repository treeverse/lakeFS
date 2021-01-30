package api

import (
	"net/http"
	"strconv"
	"time"

	"github.com/go-openapi/runtime/middleware"
	"github.com/treeverse/lakefs/httputil"
)

func MetricsHandler(ctx *middleware.Context, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		route, _, ok := ctx.RouteInfo(r)
		start := time.Now()
		mrw := httputil.NewMetricResponseWriter(w)
		next.ServeHTTP(mrw, r)
		if ok {
			requestHistograms.
				WithLabelValues(route.Operation.ID, strconv.Itoa(mrw.StatusCode)).
				Observe(time.Since(start).Seconds())
		}
	})
}
