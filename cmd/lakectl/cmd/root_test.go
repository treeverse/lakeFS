package cmd

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-openapi/swag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/osinfo"
	"github.com/treeverse/lakefs/pkg/version"
)

func TestLakectlUserAgentString(t *testing.T) {
	osInfo := osinfo.GetOSInfo()
	expectedUserAgent := fmt.Sprintf("lakectl/%s/%s/%s/%s", version.Version, osInfo.OS, osInfo.Version, osInfo.Platform)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		userAgent := r.Header.Get("User-Agent")
		assert.Equal(t, expectedUserAgent, userAgent)
	}))
	defer server.Close()

	t.Setenv("LAKECTL_SERVER_ENDPOINT_URL", server.URL)
	rootCmd.SetArgs([]string{"--version"})
	require.NoError(t, rootCmd.Execute())
}

func TestLakectlUnixPerm(t *testing.T) {
	trueP := "true"
	falseP := "false"
	testCases := []struct {
		Name   string
		EnvVal *string
	}{
		{
			Name:   "no settings",
			EnvVal: nil,
		},
		{
			Name:   "set env var false",
			EnvVal: &falseP,
		},
		{
			Name:   "set env var true",
			EnvVal: &trueP,
		},
	}
	for _, tt := range testCases {
		t.Run(tt.Name, func(t *testing.T) {
			expectedVal := swag.StringValue(tt.EnvVal)
			if len(expectedVal) > 0 {
				t.Setenv("LAKECTL_EXPERIMENTAL_LOCAL_POSIX_PERMISSIONS_ENABLED", expectedVal)
			}

			rootCmd.SetArgs([]string{"--version"})
			require.NoError(t, rootCmd.Execute())

			if expectedVal == "true" {
				require.True(t, cfg.Experimental.Local.POSIXPerm.Enabled)
			} else {
				require.False(t, cfg.Experimental.Local.POSIXPerm.Enabled)
			}
		})
	}
}
