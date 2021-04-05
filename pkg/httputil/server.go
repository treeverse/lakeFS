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

func HostsOnly(hostname []string) []string {
	ret := make([]string, len(hostname))
	for i := 0; i < len(hostname); i++ {
		ret[i] = HostOnly(hostname[i])
	}
	return ret
}

type MatchFn func(string) bool

func Exact(v []string) MatchFn {
	vHost := HostsOnly(v)
	return func(host string) bool {
		for _, v := range vHost {
			if v == host {
				return true
			}
		}
		return false
	}
}

func SubdomainsOf(v []string) MatchFn {
	subVHost := HostsOnly(v)
	for i := 0; i < len(subVHost); i++ {
		subVHost[i] = "." + subVHost[i]
	}
	return func(host string) bool {
		for _, subV := range subVHost {
			if !strings.HasSuffix(host, subV) || len(host) < len(subV)+1 {
				continue
			}
			dot := strings.IndexRune(host, '.')
			if dot > -1 && dot < len(host)-len(subV) {
				continue
			}
			return true // it is a direct sub-domain
		}
		return false
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
