package esti

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLakefsHelp(t *testing.T) {
	RunCmdAndVerifySuccessWithFile(t, Lakefs(), false, "lakefs/help", emptyVars)
	RunCmdAndVerifySuccessWithFile(t, Lakefs()+" --help", false, "lakefs/help", emptyVars)
	RunCmdAndVerifySuccessWithFile(t, Lakefs(), true, "lakefs/help", emptyVars)
	RunCmdAndVerifySuccessWithFile(t, Lakefs()+" --help", true, "lakefs/help", emptyVars)
}

func TestDDD(t *testing.T) {
	// pointing to a non-existing config file should fail
	tempDir := t.TempDir()
	nonExistingConfigPath := tempDir + "/non-existong-config.yaml"
	assert.NoFileExists(t, nonExistingConfigPath)
	runCmdAndVerifyContainsText(t, Lakefs()+" --config "+nonExistingConfigPath+" run", true, false, "Failed to find a config file", nil)

	// write a config with a recognizable listen address, then verify it is loaded
	// by checking that the address appears in the `flare --stdout` output
	configPath := tempDir + "/custom-config.yaml"
	customIP := "127.0.0.1:19991"
	configContent := fmt.Sprintf(`listen_address: "%s"
database:
  type: local
  local:
    path: %s
auth:
  encrypt:
    secret_key: "some-test-secret"
blockstore:
  type: local
  local:
    path: %s
`, customIP, tempDir, tempDir)
	require.NoError(t, os.WriteFile(configPath, []byte(configContent), 0600))
	runCmdAndVerifyContainsText(t, Lakefs()+" --config "+configPath+" flare --stdout", false, false, customIP, nil)
}

func TestLakefsSuperuser_basic(t *testing.T) {
	RequirePostgresDB(t)
	lakefsCmd := Lakefs()
	outputString := "credentials:\n  access_key_id: <ACCESS_KEY_ID>\n  secret_access_key: <SECRET_ACCESS_KEY>\n"
	username := t.Name()
	expectFailure := false
	ctx := t.Context()
	if isBasicAuth(t, ctx) {
		lakefsCmd = LakefsWithBasicAuth()
		outputString = "already exists"
		expectFailure = true
	}
	runCmdAndVerifyContainsText(t, lakefsCmd+" superuser --user-name "+username, expectFailure, false, outputString, nil)
}

func TestLakefsSuperuser_alreadyExists(t *testing.T) {
	RequirePostgresDB(t)
	lakefsCmd := Lakefs()
	ctx := t.Context()
	if isBasicAuth(t, ctx) {
		lakefsCmd = LakefsWithBasicAuth()
	}
	// On init - the AdminUsername is already created, and the expected error should be: "already exist" (also in basic auth mode)
	RunCmdAndVerifyFailureContainsText(t, lakefsCmd+" superuser --user-name "+AdminUsername, false, "already exists", nil)
}
