package esti

import "testing"

func TestLakefsHelp(t *testing.T) {
	RunCmdAndVerifySuccessWithFile(t, Lakefs(), false, "lakefs/help", emptyVars)
	RunCmdAndVerifySuccessWithFile(t, Lakefs()+" --help", false, "lakefs/help", emptyVars)
	RunCmdAndVerifySuccessWithFile(t, Lakefs(), true, "lakefs/help", emptyVars)
	RunCmdAndVerifySuccessWithFile(t, Lakefs()+" --help", true, "lakefs/help", emptyVars)
}

func TestLakefsSuperuser_basic(t *testing.T) {
	RequirePostgresDB(t)
	lakefsCmd := Lakefs()
	outputString := "credentials:\n  access_key_id: <ACCESS_KEY_ID>\n  secret_access_key: <SECRET_ACCESS_KEY>\n"
	username := t.Name()
	expectFailure := false
	if isBasicAuth() {
		lakefsCmd = LakefsWithBasicAuth()
		outputString = "already exists"
		expectFailure = true
	}
	runCmdAndVerifyContainsText(t, lakefsCmd+" superuser --user-name "+username, expectFailure, false, outputString, nil)
}

func TestLakefsSuperuser_alreadyExists(t *testing.T) {
	RequirePostgresDB(t)
	lakefsCmd := Lakefs()
	if isBasicAuth() {
		lakefsCmd = LakefsWithBasicAuth()
	}
	// On init - the AdminUsername is already created and expected error should be "already exist" (also in basic auth mode)
	RunCmdAndVerifyFailureContainsText(t, lakefsCmd+" superuser --user-name "+AdminUsername, false, "already exists", nil)
}
