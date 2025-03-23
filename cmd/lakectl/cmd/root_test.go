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
	tests := []struct {
		name        string
		setup       func(t *testing.T)
		expectedURL string
	}{
		{
			name: "using --config flag",
			setup: func(t *testing.T) {
				tmpFile := filepath.Join(t.TempDir(), "config.yaml")
				content := "server:\n  endpoint_url: \"http://flag-endpoint\"\n"
				require.NoError(t, os.WriteFile(tmpFile, []byte(content), 0644))
				cfgFile = tmpFile
			},
			expectedURL: "http://flag-endpoint",
		},
		{
			name: "using LAKECTL_CONFIG_FILE env var",
			setup: func(t *testing.T) {
				tmpFile := filepath.Join(t.TempDir(), "env_config.yaml")
				content := "server:\n  endpoint_url: \"http://env-endpoint\"\n"
				require.NoError(t, os.WriteFile(tmpFile, []byte(content), 0644))
				t.Setenv("LAKECTL_CONFIG_FILE", tmpFile)
			},
			expectedURL: "http://env-endpoint",
		},
		{
			name: "using home directory default",
			setup: func(t *testing.T) {
				homeDir := t.TempDir()
				tmpFile := filepath.Join(homeDir, ".lakectl.yaml")
				content := "server:\n  endpoint_url: \"http://home-endpoint\"\n"
				require.NoError(t, os.WriteFile(tmpFile, []byte(content), 0644))
				t.Setenv("HOME", homeDir)
			},
			expectedURL: "http://home-endpoint",
		},
		{
			name: "both --config flag and env var are set, flag should win",
			setup: func(t *testing.T) {
				tmpFileFlag := filepath.Join(t.TempDir(), "config_flag.yaml")
				flagContent := "server:\n  endpoint_url: \"http://from-flag\"\n"
				require.NoError(t, os.WriteFile(tmpFileFlag, []byte(flagContent), 0644))

				tmpFileEnv := filepath.Join(t.TempDir(), "config_env.yaml")
				envContent := "server:\n  endpoint_url: \"http://from-env\"\n"
				require.NoError(t, os.WriteFile(tmpFileEnv, []byte(envContent), 0644))

				cfgFile = tmpFileFlag
				t.Setenv("LAKECTL_CONFIG_FILE", tmpFileEnv)
			},
			expectedURL: "http://from-flag",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			viper.Reset()
			cfgFile = ""

			tt.setup(t)

			initConfig()
			require.NoError(t, viper.Unmarshal(&cfg), "Failed to unmarshal config")
			assert.Equal(t, tt.expectedURL, cfg.Server.EndpointURL.String(), "Unexpected endpoint URL")
		})
	}
}
