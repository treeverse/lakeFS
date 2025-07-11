package cmd

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/go-openapi/swag"
	"github.com/spf13/cobra"
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
				tempDir := t.TempDir()

				cfgFile = filepath.Join(tempDir, "flag.yaml")
				envPath := filepath.Join(tempDir, "env.yaml")

				const flagContent = "server:\n  endpoint_url: \"http://from-flag\"\n"
				const envContent = "server:\n  endpoint_url: \"http://from-env\"\n"

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

func TestIsUnknownCommandError(t *testing.T) {
	testRootCmd := &cobra.Command{Use: "lakectl"}
	testRootCmd.AddCommand(&cobra.Command{Use: "known"})
	testRootCmd.SetArgs([]string{"unknown-command"})
	testRootCmd.SetOut(io.Discard)
	testRootCmd.SetErr(io.Discard)

	// Verify that the command is unknown
	_, err := testRootCmd.ExecuteC()
	if !isUnknownCommandError(err) {
		t.Errorf("Expected isUnknownCommandError to return true for unknown command, got false")
	}

	// Verify that a known command does not trigger the unknown command error
	testRootCmd.SetArgs([]string{"known"})
	_, err = testRootCmd.ExecuteC()
	if isUnknownCommandError(err) {
		t.Errorf("Expected isUnknownCommandError to return false for known command, got true")
	}
}

func TestPlugin(t *testing.T) {
	type testCase struct {
		name           string
		args           []string
		expectedOutput string // Expected substring in the output (if not empty)
		expectedResult bool   // Expected boolean result from pluginExecute
	}

	// Define the test table
	tests := []testCase{
		{
			name:           "no_command",
			args:           []string{},
			expectedResult: true,
		},
		{
			name:           "known_command",
			args:           []string{"known"},
			expectedResult: true,
		},
		{
			name:           "unknown_command",
			args:           []string{"unknown"},
			expectedResult: false,
		},
		{
			name:           "plugin_command",
			args:           []string{"test-me"},
			expectedResult: true,
			expectedOutput: "test-me plugin",
		},
		{
			name:           "plugin_command_with_args",
			args:           []string{"test-me", "arg1", "arg2"},
			expectedResult: true,
			expectedOutput: "test-me plugin",
		},
	}

	// setup the test environment
	tempDir := t.TempDir()

	// Create lakectl plugin
	const pluginName = "test-me"
	pluginFile := filepath.Join(tempDir, "lakectl-"+pluginName)
	var (
		pluginContent []byte
		fileMode      os.FileMode
	)
	if runtime.GOOS == "windows" {
		pluginFile += ".bat"
		pluginContent = []byte(`echo "` + pluginName + ` plugin"`)
		fileMode = 0o644
	} else {
		pluginContent = []byte("#!/bin/sh\necho \"" + pluginName + ` plugin"`)
		fileMode = 0o755
	}
	err := os.WriteFile(pluginFile, pluginContent, fileMode)
	require.NoError(t, err, "Failed to write plugin file: %s", pluginName)

	// Set the PATH environment variable to include only the temporary directory with our plugin.
	t.Setenv("PATH", tempDir)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Save and restore os.Args to prevent side effects between test runs.
			// This is necessary because override the command args of the command using SetArgs,
			// will not pass to the plugin as the command execution fails with not found command.
			originalOsArgs := os.Args
			defer func() {
				os.Args = originalOsArgs
			}()
			os.Args = append([]string{"lakectl"}, tt.args...)

			// Setup root command with a known command
			testRootCmd := &cobra.Command{Use: "lakectl", SilenceErrors: true, SilenceUsage: true}
			testRootCmd.AddCommand(&cobra.Command{Use: "known", Short: "A known command"})
			// Capture the output of the command
			var buf bytes.Buffer
			testRootCmd.SetOut(&buf)
			testRootCmd.SetErr(&buf)

			// Run the command with plugin execution
			cmd, err := testRootCmd.ExecuteC()
			res := pluginExecute(cmd, err)

			require.Equal(t, tt.expectedResult, res)
			if tt.expectedOutput != "" {
				out := buf.String()
				require.Contains(t, out, tt.expectedOutput)
			}
		})
	}
}
