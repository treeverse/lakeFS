package httputil

import (
	"net"
	"net/http"
	"strings"

	"github.com/treeverse/lakefs/pkg/logging"
)

// GetRequestLakeFSClient get lakeFS client identifier from request.
//
//	It extracts the data from X-Lakefs-Client header and fallback to the user-agent
func GetRequestLakeFSClient(r *http.Request) string {
	id := r.Header.Get("X-Lakefs-Client")
	if id == "" {
		id = r.UserAgent()
	}
	return id
}

// ExtractClientIP extracts the client IP from the given headers
// Checks X-Forwarded-For header first (for proxy/load balancer scenarios)
func ExtractClientIP(headers http.Header, remoteAddr string) string {
	// Try X-Forwarded-For header first
	xForwardedFor := headers.Get("X-Forwarded-For")
	if xForwardedFor != "" {
		// X-Forwarded-For can contain multiple IPs, use the first one
		firstIP, _, _ := strings.Cut(xForwardedFor, ",")
		if firstIP != "" {
			clientIP := strings.TrimSpace(firstIP)
			logging.ContextUnavailable().WithField("client_ip", clientIP).Debug("Client IP extract from X-Forwarded-For")
			return clientIP
		}
	}

	// Try X-Real-IP header as fallback
	xRealIP := headers.Get("X-Real-IP")
	if xRealIP != "" {
		clientIP := strings.TrimSpace(xRealIP)
		logging.ContextUnavailable().WithField("client_ip", clientIP).Debug("Client IP extract from X-Real-IP")
		return clientIP
	}

	// Fall back to remote address, strip port if present
	if remoteAddr != "" {
		var clientIP string
		if host, _, err := net.SplitHostPort(remoteAddr); err == nil {
			clientIP = host
		} else {
			clientIP = remoteAddr
		}
		logging.ContextUnavailable().WithField("client_ip", clientIP).Debug("Client IP extract from RemoteAddr")
		return clientIP
	}

	return ""
}
