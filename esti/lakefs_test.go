package esti

import (
	"os"
	"path/filepath"
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
	// write a config with an invalid key; if --config is honored, run will fail mentioning it
	configPath := filepath.Join(t.TempDir(), "/custom-config.yaml")
	invalidKey := "invalid-key"
	require.NoError(t, os.WriteFile(configPath, []byte(invalidKey+": invalid\n"), 0600))
	runCmdAndVerifyContainsText(t, Lakefs()+" --config \""+configPath+"\" run", true, false, invalidKey, emptyVars)
}

// TestLakefsSuperuser verifies that no additional user can be added: setup creates the only supported user.
func TestLakefsSuperuser(t *testing.T) {
	RequirePostgresDB(t)
	tests := []struct {
		name     string
		userName string
	}{
		{name: "new_user", userName: "TestLakefsSuperuser"},
		{name: "existing_user", userName: AdminUsername},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			RunCmdAndVerifyFailureContainsText(t, Lakefs()+" superuser --user-name "+tt.userName, false, "already exists", nil)
		})
	}
}
