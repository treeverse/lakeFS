package httputil

import (
	"errors"
	"net/netip"
	"testing"
)

func TestAllowedHosts(t *testing.T) {
	tests := []struct {
		name    string
		entries []string
		host    string
		addr    string
		allowed bool
	}{
		{name: "public_ipv4", host: "example.com", addr: "93.184.215.14", allowed: true},
		{name: "public_ipv6", host: "example.com", addr: "2606:2800:21f:cb07:6820:80da:af6b:8b2c", allowed: true},
		{name: "loopback", host: "localhost", addr: "127.0.0.1"},
		{name: "loopback_ipv6", host: "localhost", addr: "::1"},
		{name: "loopback_ipv4_mapped", host: "x", addr: "::ffff:127.0.0.1"},
		{name: "unspecified", host: "x", addr: "0.0.0.0"},
		{name: "this_network", host: "x", addr: "0.1.2.3"},
		{name: "private_10", host: "x", addr: "10.1.2.3"},
		{name: "private_172", host: "x", addr: "172.17.0.2"},
		{name: "private_192", host: "x", addr: "192.168.1.1"},
		{name: "metadata", host: "x", addr: "169.254.169.254"},
		{name: "azure_wireserver", host: "x", addr: "168.63.129.16"},
		{name: "cgnat", host: "x", addr: "100.100.100.200"},
		{name: "nat64_metadata", host: "x", addr: "64:ff9b::a9fe:a9fe"},
		{name: "unique_local", host: "x", addr: "fd00::1"},
		{name: "link_local_ipv6_zone", host: "x", addr: "fe80::1%eth0"},
		{name: "allowed_ip", entries: []string{"127.0.0.1"}, host: "localhost", addr: "127.0.0.1", allowed: true},
		{name: "allowed_ip_other", entries: []string{"127.0.0.1"}, host: "x", addr: "127.0.0.2"},
		{name: "allowed_ip_covers_mapped", entries: []string{"127.0.0.1"}, host: "x", addr: "::ffff:127.0.0.1", allowed: true},
		{name: "allowed_cidr", entries: []string{"10.0.0.0/8"}, host: "x", addr: "10.20.30.40", allowed: true},
		{name: "allowed_cidr_other", entries: []string{"10.0.0.0/8"}, host: "x", addr: "192.168.1.1"},
		{name: "allowed_host", entries: []string{"jenkins.internal"}, host: "Jenkins.Internal.", addr: "10.1.2.3", allowed: true},
		{name: "allowed_host_glob", entries: []string{"*.corp.example"}, host: "hooks.corp.example", addr: "10.1.2.3", allowed: true},
		{name: "allowed_host_uppercase_entry", entries: []string{"*.example.NET"}, host: "ariels.example.NET", addr: "10.1.2.3", allowed: true},
		{name: "allowed_host_other", entries: []string{"*.corp.example"}, host: "corp.example.evil", addr: "10.1.2.3"},
		{name: "allow_all", entries: []string{"0.0.0.0/0", "::/0"}, host: "x", addr: "169.254.169.254", allowed: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a, err := ParseAllowedHosts(tt.entries)
			if err != nil {
				t.Fatalf("ParseAllowedHosts(%v): %s", tt.entries, err)
			}
			if got := a.allowed(tt.host, netip.MustParseAddr(tt.addr)); got != tt.allowed {
				t.Errorf("allowed(%s, %s) = %t, expected %t", tt.host, tt.addr, got, tt.allowed)
			}
		})
	}
}

func TestParseAllowedHosts_Invalid(t *testing.T) {
	for _, entry := range []string{
		"[bad",
		"localhost,10.0.0.0/8", // comma-separated list in an environment variable
		"localhost 10.0.0.0/8",
		"::ffff:127.0.0.1",
		"[::ffff:127.0.0.1]",
		"::ffff:127.0.0.0/104",
	} {
		t.Run(entry, func(t *testing.T) {
			if _, err := ParseAllowedHosts([]string{entry}); !errors.Is(err, ErrInvalidAllowedHost) {
				t.Errorf("ParseAllowedHosts(%q) = %v, expected ErrInvalidAllowedHost", entry, err)
			}
		})
	}
}
