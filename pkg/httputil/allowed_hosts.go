package httputil

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"path"
	"strings"
	"syscall"
	"time"
)

var (
	ErrHostNotAllowed     = errors.New("destination host not allowed")
	ErrInvalidAllowedHost = errors.New("invalid allowed host")

	errIPv4Mapped = errors.New("IPv4-mapped IPv6 address, use the IPv4 form")
)

// Transport settings match those of http.DefaultTransport.
const (
	allowedHostsDialTimeout         = 30 * time.Second
	allowedHostsKeepAlive           = 30 * time.Second
	allowedHostsTLSHandshakeTimeout = 10 * time.Second
	allowedHostsIdleConnTimeout     = 90 * time.Second
)

// internalNetworks are destinations refused unless explicitly allowed: loopback, private,
// link-local (cloud metadata), and special-purpose ranges, including IPv6 ranges that embed IPv4.
var internalNetworks = func() []netip.Prefix {
	cidrs := []string{
		"0.0.0.0/8",        // "this network"
		"10.0.0.0/8",       // RFC 1918 private
		"100.64.0.0/10",    // RFC 6598 carrier-grade NAT
		"127.0.0.0/8",      // loopback
		"168.63.129.16/32", // Azure WireServer
		"169.254.0.0/16",   // link-local, cloud metadata
		"172.16.0.0/12",    // RFC 1918 private
		"192.0.0.0/24",     // IETF protocol assignments
		"192.0.2.0/24",     // TEST-NET-1
		"192.88.99.0/24",   // 6to4 relay anycast
		"192.168.0.0/16",   // RFC 1918 private
		"198.18.0.0/15",    // benchmarking
		"198.51.100.0/24",  // TEST-NET-2
		"203.0.113.0/24",   // TEST-NET-3
		"224.0.0.0/4",      // multicast
		"240.0.0.0/4",      // reserved, broadcast
		"::/96",            // unspecified, loopback, IPv4-compatible
		"64:ff9b::/96",     // NAT64
		"64:ff9b:1::/48",   // local-use NAT64
		"100::/64",         // discard-only
		"2001::/32",        // Teredo
		"2001:10::/28",     // ORCHID
		"2001:20::/28",     // ORCHIDv2
		"2001:db8::/32",    // documentation
		"2002::/16",        // 6to4
		"fc00::/7",         // unique local
		"fe80::/10",        // link-local
		"fec0::/10",        // site-local (deprecated)
		"ff00::/8",         // multicast
	}
	prefixes := make([]netip.Prefix, len(cidrs))
	for i, cidr := range cidrs {
		prefixes[i] = netip.MustParsePrefix(cidr)
	}
	return prefixes
}()

// AllowedHosts restricts outbound connections to internal networks. Public addresses are always
// allowed; an internal address is allowed only when it matches an allowed IP/CIDR entry, or the
// requested host name matches an allowed host name pattern.
type AllowedHosts struct {
	prefixes []netip.Prefix
	patterns []string
}

// ParseAllowedHosts parses entries of IP addresses, CIDR ranges and host name patterns
// (path.Match syntax, e.g. "*.example.com").  Entries that could never match are refused: a list
// packed into one entry, and IPv4-mapped IPv6 addresses (destinations are matched in IPv4 form).
func ParseAllowedHosts(entries []string) (*AllowedHosts, error) {
	a := &AllowedHosts{}
	for _, entry := range entries {
		entry = strings.ToLower(strings.TrimSpace(entry))
		if entry == "" {
			continue
		}
		if strings.ContainsAny(entry, ", \t\n") {
			return nil, fmt.Errorf(`%w %q: one host per entry; in an environment variable use a JSON array, e.g. ["10.0.0.0/8","jenkins.internal"]`, ErrInvalidAllowedHost, entry)
		}
		if prefix, err := netip.ParsePrefix(entry); err == nil {
			if prefix.Addr().Is4In6() {
				return nil, fmt.Errorf("%w %q: %w", ErrInvalidAllowedHost, entry, errIPv4Mapped)
			}
			a.prefixes = append(a.prefixes, prefix.Masked())
			continue
		}
		if addr, err := netip.ParseAddr(strings.Trim(entry, "[]")); err == nil {
			if addr.Is4In6() {
				return nil, fmt.Errorf("%w %q: %w", ErrInvalidAllowedHost, entry, errIPv4Mapped)
			}
			a.prefixes = append(a.prefixes, netip.PrefixFrom(addr, addr.BitLen()))
			continue
		}
		if _, err := path.Match(entry, ""); err != nil {
			return nil, fmt.Errorf("%w %q: %w", ErrInvalidAllowedHost, entry, err)
		}
		a.patterns = append(a.patterns, entry)
	}
	return a, nil
}

func (a *AllowedHosts) allowed(host string, addr netip.Addr) bool {
	addr = addr.Unmap().WithZone("")
	if !isInternal(addr) {
		return true
	}
	for _, prefix := range a.prefixes {
		if prefix.Contains(addr) {
			return true
		}
	}
	host = strings.TrimSuffix(strings.ToLower(host), ".")
	for _, pattern := range a.patterns {
		if matched, _ := path.Match(pattern, host); matched {
			return true
		}
	}
	return false
}

func isInternal(addr netip.Addr) bool {
	for _, prefix := range internalNetworks {
		if prefix.Contains(addr) {
			return true
		}
	}
	return false
}

// DialContext dials addr, checking every resolved address just before connecting to it, so
// DNS rebinding and redirects cannot reach a destination that is not allowed.
func (a *AllowedHosts) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	dialer := &net.Dialer{
		Timeout:   allowedHostsDialTimeout,
		KeepAlive: allowedHostsKeepAlive,
		Control: func(_, address string, _ syscall.RawConn) error {
			addrPort, err := netip.ParseAddrPort(address)
			if err != nil {
				return fmt.Errorf("%w: %s: %w", ErrHostNotAllowed, address, err)
			}
			if !a.allowed(host, addrPort.Addr()) {
				return fmt.Errorf("%w: %s (%s)", ErrHostNotAllowed, host, addrPort.Addr())
			}
			return nil
		},
	}
	return dialer.DialContext(ctx, network, addr)
}

// Transport returns an HTTP transport whose connections, including redirects, are checked.
// Proxy environment variables are ignored, as a proxy would hide the destination from the check.
// Connections are checked when dialed and reused only for the same host and port, so pooling is
// safe; callers discarding the transport should call CloseIdleConnections.
func (a *AllowedHosts) Transport() *http.Transport {
	return &http.Transport{
		DialContext:         a.DialContext,
		ForceAttemptHTTP2:   true,
		IdleConnTimeout:     allowedHostsIdleConnTimeout,
		TLSHandshakeTimeout: allowedHostsTLSHandshakeTimeout,
	}
}
