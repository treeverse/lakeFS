package cmd

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-openapi/swag"
	"github.com/spf13/viper"
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

func TestInitConfig_LoadingScenarios(t *testing.T) {
	type setupFn func(t *testing.T)

	tests := []struct {
		name        string
		setup       setupFn
		expectedURL string
	}{
		{
			name: "flag",
			setup: func(t *testing.T) {
				t.Helper()
				const content = "server:\n  endpoint_url: \"http://flag-endpoint\"\n"
				cfgFile = filepath.Join(t.TempDir(), "config.yaml")
				require.NoError(t, os.WriteFile(cfgFile, []byte(content), 0644))
			},
			expectedURL: "http://flag-endpoint",
		},
		{
			name: "env",
			setup: func(t *testing.T) {
				t.Helper()
				const content = "server:\n  endpoint_url: \"http://env-endpoint\"\n"
				path := filepath.Join(t.TempDir(), "env_config.yaml")
				require.NoError(t, os.WriteFile(path, []byte(content), 0644))
				t.Setenv("LAKECTL_CONFIG_FILE", path)
			},
			expectedURL: "http://env-endpoint",
		},
		{
			name: "home",
			setup: func(t *testing.T) {
				t.Helper()
				const content = "server:\n  endpoint_url: \"http://home-endpoint\"\n"
				home := t.TempDir()
				require.NoError(t, os.WriteFile(filepath.Join(home, ".lakectl.yaml"), []byte(content), 0644))
				t.Setenv("HOME", home)
			},
			expectedURL: "http://home-endpoint",
		},
		{
			name: "flag_and_env",
			setup: func(t *testing.T) {
				t.Helper()
				const flagContent = "server:\n  endpoint_url: \"http://from-flag\"\n"
				const envContent = "server:\n  endpoint_url: \"http://from-env\"\n"

				cfgFile = filepath.Join(t.TempDir(), "flag.yaml")
				envPath := filepath.Join(t.TempDir(), "env.yaml")

				require.NoError(t, os.WriteFile(cfgFile, []byte(flagContent), 0644))
				require.NoError(t, os.WriteFile(envPath, []byte(envContent), 0644))

				t.Setenv("LAKECTL_CONFIG_FILE", envPath)
			},
			expectedURL: "http://from-flag",
		},
	}

	originalCfgFile := cfgFile
	defer func() {
		cfgFile = originalCfgFile
		initConfig()
	}()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			viper.Reset()
			cfgFile = ""

			tt.setup(t)
			initConfig()

			var localCfg Configuration
			require.NoError(t, viper.Unmarshal(&localCfg), "Failed to unmarshal config")

			assert.Equal(t, tt.expectedURL, localCfg.Server.EndpointURL.String())
		})
	}
}
