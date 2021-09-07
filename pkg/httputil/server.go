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

func HostMatches(r *http.Request, hosts []string) bool {
	host := HostOnly(r.Host)
	vHost := HostsOnly(hosts)
	for _, v := range vHost {
		if v == host {
			return true
		}
	}
	return false
}

func HostSubdomainOf(r *http.Request, hosts []string) bool {
	host := HostOnly(r.Host)
	subVHost := HostsOnly(hosts)
	for i := 0; i < len(subVHost); i++ {
		subVHost[i] = "." + subVHost[i]
	}
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
