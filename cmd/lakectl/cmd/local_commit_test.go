package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/deepmap/oapi-codegen/pkg/securityprovider"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
	"github.com/treeverse/lakefs/pkg/local"
	"github.com/treeverse/lakefs/pkg/uri"
)

const (
	defaultAdminAccessKeyID     = "AKIAIOSFDNN7EXAMPLEQ"
	defaultAdminSecretAccessKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
)

func getTestClient(t *testing.T, endpoint string) *apigen.ClientWithResponses {
	t.Helper()
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConnsPerHost = DefaultMaxIdleConnsPerHost
	httpClient := &http.Client{
		Transport: transport,
	}
	basicAuthProvider, err := securityprovider.NewSecurityProviderBasicAuth(string(defaultAdminAccessKeyID), string(defaultAdminSecretAccessKey))
	require.NoError(t, err)

	serverEndpoint, err := apiutil.NormalizeLakeFSEndpoint(endpoint)
	require.NoError(t, err)

	client, err := apigen.NewClientWithResponses(
		serverEndpoint,
		apigen.WithHTTPClient(httpClient),
		apigen.WithRequestEditorFn(basicAuthProvider.Intercept),
	)
	require.NoError(t, err)

	return client
}

func TestUncommittedOutsideOfPrefix(t *testing.T) {
	prefix := "xyzzy/"
	remote := &uri.URI{
		Repository: "test",
		Ref:        "test",
	}
	idx := &local.Index{
		PathURI:         fmt.Sprintf("lakefs://test/test/%s", prefix),
		ActiveOperation: "",
	}

	testCases := []struct {
		name           string
		h              http.HandlerFunc
		expectedResult bool
	}{
		{
			name: "Uncommitted changes - none",
			h: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				res := &apigen.DiffList{
					Results: []apigen.Diff{},
				}
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("X-Content-Type-Options", "nosniff")
				w.WriteHeader(http.StatusOK)
				json.NewEncoder(w).Encode(res)
			}),
			expectedResult: false,
		},
		{
			name: "Uncommitted changes - outside",
			h: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				res := &apigen.DiffList{
					Results: []apigen.Diff{
						{
							PathType: "object",
							Path:     "otherPrefix/a",
						},
					},
				}
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("X-Content-Type-Options", "nosniff")
				w.WriteHeader(http.StatusOK)
				json.NewEncoder(w).Encode(res)
			}),
			expectedResult: true,
		},
		{
			name: "Uncommitted changes - inside",
			h: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var res *apigen.DiffList
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("X-Content-Type-Options", "nosniff")
				if strings.Contains(r.RequestURI, "/diff?amount") {
					res = &apigen.DiffList{
						Results: []apigen.Diff{
							{
								PathType: "object",
								Path:     fmt.Sprintf("%sa", prefix),
							},
						},
					}
					w.WriteHeader(http.StatusOK)
				}

				if strings.Contains(r.RequestURI, "/diff?after") {
					res = &apigen.DiffList{
						Results: []apigen.Diff{},
					}
					w.WriteHeader(http.StatusOK)
				}

				if strings.Contains(r.RequestURI, "/stat?path") {
					res = &apigen.DiffList{}
					w.WriteHeader(http.StatusNotFound)
				}
				require.NotNil(t, res, "Unexpected request")
				json.NewEncoder(w).Encode(res)
			}),
			expectedResult: false,
		},
		{
			name: "Uncommitted changes - inside before outside",
			h: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var res *apigen.DiffList
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("X-Content-Type-Options", "nosniff")
				if strings.Contains(r.RequestURI, "/diff?amount") {
					res = &apigen.DiffList{
						Results: []apigen.Diff{
							{
								PathType: "object",
								Path:     fmt.Sprintf("%sa", prefix),
							},
						},
					}
					w.WriteHeader(http.StatusOK)
				}

				if strings.Contains(r.RequestURI, "/diff?after") {
					res = &apigen.DiffList{
						Results: []apigen.Diff{
							{
								PathType: "object",
								Path:     "zzz/a",
							},
						},
					}
					w.WriteHeader(http.StatusOK)
				}

				if strings.Contains(r.RequestURI, "/stat?path") {
					res = &apigen.DiffList{}
					w.WriteHeader(http.StatusNotFound)
				}

				require.NotNil(t, res, "Unexpected request")
				json.NewEncoder(w).Encode(res)
			}),
			expectedResult: true,
		},
		{
			name: "Uncommitted changes - on the boundry",
			h: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var res *apigen.DiffList
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("X-Content-Type-Options", "nosniff")
				if strings.Contains(r.RequestURI, "/diff?amount") {
					res = &apigen.DiffList{
						Results: []apigen.Diff{
							{
								PathType: "object",
								Path:     fmt.Sprintf("%sa", prefix),
							},
						},
					}
					w.WriteHeader(http.StatusOK)
				}

				if strings.Contains(r.RequestURI, "/diff?after") {
					res = &apigen.DiffList{
						Results: []apigen.Diff{},
					}
					w.WriteHeader(http.StatusOK)
				}

				if strings.Contains(r.RequestURI, "/stat?path") {
					// we only look at the status, so we don't care about the body
					res = &apigen.DiffList{}
					w.WriteHeader(http.StatusOK)
				}

				require.NotNil(t, res, "Unexpected request")
				json.NewEncoder(w).Encode(res)
			}),
			expectedResult: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(tc.h)
			defer server.Close()

			testClient := getTestClient(t, server.URL)
			res := hasExternalChange(context.Background(), testClient, remote, idx)
			require.Equal(t, tc.expectedResult, res)
		})
	}
}
