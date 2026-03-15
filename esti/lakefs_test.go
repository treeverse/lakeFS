package esti

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLakefsHelp(t *testing.T) {
	RunCmdAndVerifySuccessWithFile(t, Lakefs(), false, "lakefs/help", emptyVars)
	RunCmdAndVerifySuccessWithFile(t, Lakefs()+" --help", false, "lakefs/help", emptyVars)
	RunCmdAndVerifySuccessWithFile(t, Lakefs(), true, "lakefs/help", emptyVars)
	RunCmdAndVerifySuccessWithFile(t, Lakefs()+" --help", true, "lakefs/help", emptyVars)
}

func TestLakefsConfig(t *testing.T) {
	tempDir := t.TempDir()
	// write a config with an invalid key; if --config is honored, run will fail mentioning it
	configPath := tempDir + "/custom-config.yaml"
	invalidKey := "invalid-key"
	configContent := fmt.Sprintf(`database:
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
%s : invalid
`, tempDir, tempDir, invalidKey)
	require.NoError(t, os.WriteFile(configPath, []byte(configContent), 0600))
	runCmdAndVerifyContainsText(t, Lakefs()+" --config "+configPath+" run", true, false, invalidKey, nil)
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
