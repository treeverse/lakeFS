package httputil

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/pprof"
	"strings"

	"github.com/treeverse/lakefs/pkg/health"
)

var healthInfo string

func SetHealthHandlerInfo(info string) {
	healthInfo = info
}

func ServeHealth(healthService health.Service, ctx context.Context) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := json.MarshalIndent(healthService.Health(ctx), "", "\t")
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_, _ = io.WriteString(w, "alive!")
		if healthInfo != "" {
			_, _ = io.WriteString(w, " "+healthInfo)
		}
	})
}

func ServePPROF(pprofPrefix string) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		endpoint := strings.TrimPrefix(request.URL.Path, pprofPrefix)
		switch endpoint {
		case "":
			http.HandlerFunc(pprof.Index).ServeHTTP(writer, request)
		case "cmdline":
			http.HandlerFunc(pprof.Cmdline).ServeHTTP(writer, request)
		case "profile":
			http.HandlerFunc(pprof.Profile).ServeHTTP(writer, request)
		case "symbol":
			http.HandlerFunc(pprof.Symbol).ServeHTTP(writer, request)
		case "trace":
			http.HandlerFunc(pprof.Trace).ServeHTTP(writer, request)
		case "allocs", "block", "goroutine", "heap", "mutex", "threadcreate":
			pprof.Handler(endpoint).ServeHTTP(writer, request)
		default:
			writer.WriteHeader(http.StatusNotFound)
		}
	})
}
