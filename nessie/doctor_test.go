package nessie

import (
	"os"
	"testing"

	"github.com/spf13/viper"
)

func TestDoctor(t *testing.T) {
	accessKeyID := viper.GetString("access_key_id")
	secretAccessKey := viper.GetString("secret_access_key")
	endPointURL := viper.GetString("endpoint_url") + "/api/v1"

	defaultConfigPath := "/root/.lakectl.yaml"
	err := os.WriteFile(defaultConfigPath, []byte{}, 0600)
	if err != nil {
		t.Fatalf("failed to create lakectl default config: %s", err)
	}

	RunCmdAndVerifySuccessWithFile(t, LakectlWithParams(accessKeyID, secretAccessKey, endPointURL)+" doctor", false, "lakectl_doctor_ok", emptyVars)
	RunCmdAndVerifyFailureWithFile(t, lakectlLocation()+" doctor -c not_exits.yaml", false, "lakectl_doctor_not_exists_file", emptyVars)
	RunCmdAndVerifySuccessWithFile(t, LakectlWithParams(accessKeyID, secretAccessKey, endPointURL+"1")+" doctor", false, "lakectl_doctor_wrong_endpoint", emptyVars)
	RunCmdAndVerifySuccessWithFile(t, LakectlWithParams(accessKeyID, secretAccessKey, "wrong_uri")+" doctor", false, "lakectl_doctor_wrong_uri_format_endpoint", emptyVars)
	RunCmdAndVerifySuccessWithFile(t, LakectlWithParams("AKIAJZZZZZZZZZZZZZZQ", secretAccessKey, endPointURL)+" doctor", false, "lakectl_doctor_with_wrong_credentials", emptyVars)
	RunCmdAndVerifySuccessWithFile(t, LakectlWithParams("AKIAJOI!COZ5JBYHCSDQ", secretAccessKey, endPointURL)+" doctor", false, "lakectl_doctor_with_suspicious_access_key_id", emptyVars)
	RunCmdAndVerifySuccessWithFile(t, LakectlWithParams(accessKeyID, "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz", endPointURL)+" doctor", false, "lakectl_doctor_with_wrong_credentials", emptyVars)
	RunCmdAndVerifySuccessWithFile(t, LakectlWithParams(accessKeyID, "TQG5JcovOozCGJnIRmIKH7Flq1tLxnuByi9/WmJ!", endPointURL)+" doctor", false, "lakectl_doctor_with_suspicious_secret_access_key", emptyVars)

	RunCmdAndVerifySuccessWithFile(t, LakectlWithParams(accessKeyID, secretAccessKey, endPointURL)+" doctor --verbose", false, "lakectl_doctor_ok_verbose", emptyVars)
	RunCmdAndVerifyFailureWithFile(t, lakectlLocation()+" doctor -c not_exits.yaml --verbose", false, "lakectl_doctor_not_exists_file", emptyVars)
	RunCmdAndVerifySuccessWithFile(t, LakectlWithParams(accessKeyID, secretAccessKey, endPointURL+"1")+" doctor --verbose", false, "lakectl_doctor_wrong_endpoint_verbose", emptyVars)
	RunCmdAndVerifySuccessWithFile(t, LakectlWithParams(accessKeyID, secretAccessKey, "wrong_uri")+" doctor --verbose", false, "lakectl_doctor_wrong_uri_format_endpoint_verbose", emptyVars)
	RunCmdAndVerifySuccessWithFile(t, LakectlWithParams("AKIAJZZZZZZZZZZZZZZQ", secretAccessKey, endPointURL)+" doctor --verbose", false, "lakectl_doctor_with_wrong_credentials_verbose", emptyVars)
	RunCmdAndVerifySuccessWithFile(t, LakectlWithParams("AKIAJOI!COZ5JBYHCSDQ", secretAccessKey, endPointURL)+" doctor --verbose", false, "lakectl_doctor_with_suspicious_access_key_id_verbose", emptyVars)
	RunCmdAndVerifySuccessWithFile(t, LakectlWithParams(accessKeyID, "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz", endPointURL)+" doctor --verbose", false, "lakectl_doctor_with_wrong_credentials_verbose", emptyVars)
	RunCmdAndVerifySuccessWithFile(t, LakectlWithParams(accessKeyID, "TQG5JcovOozCGJnIRmIKH7Flq1tLxnuByi9/WmJ!", endPointURL)+" doctor --verbose", false, "lakectl_doctor_with_suspicious_secret_access_key_verbose", emptyVars)
}
