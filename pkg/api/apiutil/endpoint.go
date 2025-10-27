package apiutil

import (
	"net/url"
	"strings"
)

// NormalizeLakeFSEndpoint verify and return the endpoint for the lakeFS server
func NormalizeLakeFSEndpoint(endpoint string) (string, error) {
	u, err := url.Parse(endpoint)
	if err != nil {
		return "", err
	}
	// if no uri to api is set in configuration - set the default
	if u.Path == "" || u.Path == "/" {
		endpoint = strings.TrimRight(endpoint, "/") + BaseURL
	}
	return endpoint, nil
}
