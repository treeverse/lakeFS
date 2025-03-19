package esti

import (
	"fmt"
	"net/url"
	"strings"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
)

func TestLakectlDoctor(t *testing.T) {
	accessKeyID := viper.GetString("access_key_id")
	secretAccessKey := viper.GetString("secret_access_key")
	endPointURL := strings.TrimSuffix(viper.GetString("endpoint_url"), "/") + apiutil.BaseURL
	u, err := url.Parse(endpointURL)
	require.NoError(t, err)
	vars := map[string]string{
		"LAKEFS_ENDPOINT": endPointURL,
		"HOST":            fmt.Sprintf("%s://%s", u.Scheme, u.Host),
	}

	RunCmdAndVerifySuccessWithFile(t, LakectlWithParams(accessKeyID, secretAccessKey, endPointURL)+" doctor", false, "lakectl_doctor_ok", vars)
	RunCmdAndVerifyFailureWithFile(t, lakectlLocation()+" doctor -c not_exits.yaml", false, "lakectl_doctor_not_exists_file", vars)
	RunCmdAndVerifySuccessWithFile(t, LakectlWithParams(accessKeyID, secretAccessKey, endPointURL+"1")+" doctor", false, "lakectl_doctor_wrong_endpoint", vars)
	RunCmdAndVerifySuccessWithFile(t, LakectlWithParams(accessKeyID, secretAccessKey, "wrong_uri")+" doctor", false, "lakectl_doctor_wrong_uri_format_endpoint", vars)
	RunCmdAndVerifySuccessWithFile(t, LakectlWithParams("AKIAJZZZZZZZZZZZZZZQ", secretAccessKey, endPointURL)+" doctor", false, "lakectl_doctor_with_wrong_credentials", vars)
	RunCmdAndVerifySuccessWithFile(t, LakectlWithParams("AKIAJOI!COZ5JBYHCSDQ", secretAccessKey, endPointURL)+" doctor", false, "lakectl_doctor_with_suspicious_access_key_id", vars)
	RunCmdAndVerifySuccessWithFile(t, LakectlWithParams(accessKeyID, "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz", endPointURL)+" doctor", false, "lakectl_doctor_with_wrong_credentials", vars)
	RunCmdAndVerifySuccessWithFile(t, LakectlWithParams(accessKeyID, "TQG5JcovOozCGJnIRmIKH7Flq1tLxnuByi9/WmJ!", endPointURL)+" doctor", false, "lakectl_doctor_with_suspicious_secret_access_key", vars)

	RunCmdAndVerifySuccessWithFile(t, LakectlWithParams(accessKeyID, secretAccessKey, endPointURL)+" doctor --verbose", false, "lakectl_doctor_ok_verbose", vars)
	RunCmdAndVerifyFailureWithFile(t, lakectlLocation()+" doctor -c not_exits.yaml --verbose", false, "lakectl_doctor_not_exists_file", vars)
	RunCmdAndVerifySuccessWithFile(t, LakectlWithParams(accessKeyID, secretAccessKey, endPointURL+"1")+" doctor --verbose", false, "lakectl_doctor_wrong_endpoint_verbose", vars)
	RunCmdAndVerifySuccessWithFile(t, LakectlWithParams(accessKeyID, secretAccessKey, "wrong_uri")+" doctor --verbose", false, "lakectl_doctor_wrong_uri_format_endpoint_verbose", vars)
	RunCmdAndVerifySuccessWithFile(t, LakectlWithParams("AKIAJZZZZZZZZZZZZZZQ", secretAccessKey, endPointURL)+" doctor --verbose", false, "lakectl_doctor_with_wrong_credentials_verbose", vars)
	RunCmdAndVerifySuccessWithFile(t, LakectlWithParams("AKIAJOI!COZ5JBYHCSDQ", secretAccessKey, endPointURL)+" doctor --verbose", false, "lakectl_doctor_with_suspicious_access_key_id_verbose", vars)
	RunCmdAndVerifySuccessWithFile(t, LakectlWithParams(accessKeyID, "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz", endPointURL)+" doctor --verbose", false, "lakectl_doctor_with_wrong_credentials_verbose", vars)
	RunCmdAndVerifySuccessWithFile(t, LakectlWithParams(accessKeyID, "TQG5JcovOozCGJnIRmIKH7Flq1tLxnuByi9/WmJ!", endPointURL)+" doctor --verbose", false, "lakectl_doctor_with_suspicious_secret_access_key_verbose", vars)
}
