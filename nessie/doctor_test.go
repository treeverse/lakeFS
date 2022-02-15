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
	os.Create(defaultConfigPath)
	RunCmdAndVerifySuccessWithFile(t, LakectlWithParams(accessKeyID, secretAccessKey, endPointURL)+" doctor", false, "lakectl_doctor_ok", emptyVars)
	RunCmdAndVerifyFailureWithFile(t, lakectlLocation()+" doctor -c not_exited.yaml", false, "lakectl_doctor_not_existed_file", emptyVars)
	RunCmdAndVerifyFailureWithFile(t, LakectlWithParams(accessKeyID, secretAccessKey, endPointURL+"1")+" doctor", false, "lakectl_doctor_wrong_endpoint", emptyVars)
	RunCmdAndVerifyFailureWithFile(t, LakectlWithParams(accessKeyID, secretAccessKey, "wrong_uri")+" doctor", false, "lakectl_doctor_wrong_uri_format_endpoint", emptyVars)
	RunCmdAndVerifyFailureWithFile(t, LakectlWithParams("AKIAJZZZZZZZZZZZZZZQ", secretAccessKey, endPointURL)+" doctor", false, "lakectl_doctor_wrong_credentials", emptyVars)
	RunCmdAndVerifyFailureWithFile(t, LakectlWithParams("AKIAJOI!COZ5JBYHCSDQ", secretAccessKey, endPointURL)+" doctor", false, "lakectl_doctor_with_suspicious_access_key_id", emptyVars)
	RunCmdAndVerifyFailureWithFile(t, LakectlWithParams(accessKeyID, "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz", endPointURL)+" doctor", false, "lakectl_doctor_with_wrong_credentials", emptyVars)
	RunCmdAndVerifyFailureWithFile(t, LakectlWithParams(accessKeyID, "TQG5JcovOozCGJnIRmIKH7Flq1tLxnuByi9/WmJ!", endPointURL)+" doctor", false, "lakectl_doctor_with_suspicious_secret_access_key", emptyVars)

}
