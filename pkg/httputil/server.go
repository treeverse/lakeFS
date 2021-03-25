package httputil

import (
	"net"
	"net/http"
	"strings"
)

func HostOnly(hostname string) string {
	if strings.Contains(hostname, ":") {
		host, _, _ := net.SplitHostPort(hostname)
		return host
	}
	return hostname
}

type MatchFn func(string) bool

func Exact(v string) MatchFn {
	vHost := HostOnly(v)
	return func(host string) bool {
		return vHost == host
	}
}

func SubdomainsOf(v string) MatchFn {
	subV := "." + HostOnly(v)
	return func(host string) bool {
		if !strings.HasSuffix(host, subV) || len(host) < len(subV)+1 {
			return false
		}
		dot := strings.IndexRune(host, '.')
		if dot > -1 && dot < len(host)-len(subV) {
			return false
		}
		return true // it is a direct sub-domain
	}
}

type HostMuxHandler struct {
	MatchFns []MatchFn
	Handler  http.Handler

	isDefault bool
}

func (h *HostMuxHandler) Default() *HostMuxHandler {
	h.isDefault = true
	return h
}

func HostHandler(handler http.Handler, hostPatterns ...MatchFn) *HostMuxHandler {
	return &HostMuxHandler{
		MatchFns: hostPatterns,
		Handler:  handler,
	}
}

// HostMux find the default handler
func HostMux(handlers ...*HostMuxHandler) http.Handler {
	defaultHandler := handlers[0].Handler
	for _, handler := range handlers {
		if handler.isDefault {
			defaultHandler = handler.Handler
			break
		}
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host := HostOnly(r.Host)
		matchedHandler := defaultHandler
		for _, handler := range handlers {
			for _, matchFn := range handler.MatchFns {
				if matchFn(host) {
					matchedHandler = handler.Handler
					break
				}
			}
		}
		matchedHandler.ServeHTTP(w, r) // do actual request
	})
}
