package nessie

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

var update = flag.Bool("update", false, "update golden files with results")

func lakectl() string {
	os.Setenv("LAKECTL_DIR", "~/Code/lakeFS")
	lakectlCmdline :=
		"LAKECTL_CREDENTIALS_ACCESS_KEY_ID=" + viper.GetString("access_key_id") +
			" LAKECTL_CREDENTIALS_SECRET_ACCESS_KEY=" + viper.GetString("secret_access_key") +
			" LAKECTL_SERVER_ENDPOINT_URL=" + viper.GetString("endpoint_url") +
			" " + viper.GetString("lakectl_dir") + "/lakectl"

	return lakectlCmdline
}

func runShellCommand(command string, isTerminal bool) ([]byte, error) {
	fmt.Println("Testing '", command, "'")
	var cmd exec.Cmd
	//
	// Assuming linux. Not sure this is correct
	//
	if isTerminal {
		cmd = *exec.Command("/usr/bin/script", "--return", "--quiet", "-c", command, "/dev/null")
	} else {
		cmd = *exec.Command("/bin/sh", "-c", command)
	}

	return cmd.CombinedOutput()
}

//
// Copied and slightly modified from go-cmdtest
//
var varRegexp = regexp.MustCompile(`\$\{([^${}]+)\}`)

func expandVariables(s string, vars map[string]string) (string, error) {
	var sb strings.Builder
	for {
		ixs := varRegexp.FindStringSubmatchIndex(s)
		if ixs == nil {
			sb.WriteString(s)
			return sb.String(), nil
		}
		varName := s[ixs[2]:ixs[3]]
		varVal, ok := vars[varName]
		if !ok {
			return "", fmt.Errorf("variable %q not found", varName)
		}
		sb.WriteString(s[:ixs[0]])
		sb.WriteString(varVal)
		s = s[ixs[1]:]
	}
}

func embedVariables(s string, vars map[string]string) (string, error) {
	revMap := make(map[string]string)
	vals := make([]string, 0, len(vars)) //collecting all vals, which will be used as keys, in order to control iteration order

	for k, v := range vars {
		if _, exist := revMap[v]; exist {
			return "", fmt.Errorf("value %q not unique in mapping", v)
		}
		revMap[v] = k
		vals = append(vals, v)
	}

	//
	// Sorting the reversed keys (variable values) by descending length in order to handle longer nbames first
	// This will diminish replacing partial names that were used to construct longer names
	//
	sort.Slice(vals, func(i, j int) bool {
		return len(vals[i]) > len(vals[j])
	})

	for _, val := range vals {
		s = strings.Replace(s, val, "${"+revMap[val]+"}", -1)
	}

	return s, nil
}

func sanitize(output string) string {
	s := removeProgramTimestamp(output)
	return strings.ReplaceAll(s, "\r\n", "\n")

}

func runCmdAndVerifySuccessWithFile(t *testing.T, cmd string, isTerminal bool, goldenFile string, vars map[string]string) {
	runCmdAndVerifyWithFile(t, cmd, false, isTerminal, goldenFile, vars)
}

func runCmdAndVerifyFailureWithFile(t *testing.T, cmd string, isTerminal bool, goldenFile string, vars map[string]string) {
	runCmdAndVerifyWithFile(t, cmd, true, isTerminal, goldenFile, vars)
}

func runCmdAndVerifyWithFile(t *testing.T, cmd string, expectFail bool, isTerminal bool, goldenFile string, vars map[string]string) {
	goldenFile = "golden/" + goldenFile + ".golden"

	if *update {
		result, _ := runShellCommand(cmd, isTerminal)
		s, err := embedVariables(string(result), vars)
		require.NoError(t, err, "Variable embed failed - %s", err)
		err = ioutil.WriteFile(goldenFile, []byte(sanitize(s)), 0777)
		require.NoError(t, err, "Failed to write file %s", goldenFile)
		return
	}
	content, err := ioutil.ReadFile(goldenFile)
	if err != nil {
		t.Fatal("Failed to read ", goldenFile, err)
	}
	expected := sanitize(string(content))
	runCmdAndVerifyResult(t, cmd, expectFail, isTerminal, expected, vars)
}

func runCmdAndVerifySuccess(t *testing.T, cmd string, isTerminal bool, expected string, vars map[string]string) {
	runCmdAndVerifyResult(t, cmd, false, isTerminal, expected, vars)
}

func runCmdAndVerifyFailure(t *testing.T, cmd string, isTerminal bool, expected string, vars map[string]string) {
	runCmdAndVerifyResult(t, cmd, true, isTerminal, expected, vars)
}

func runCmdAndVerifyResult(t *testing.T, cmd string, expectFail bool, isTerminal bool, expected string, vars map[string]string) {
	expanded, err := expandVariables(expected, vars)
	if err != nil {
		t.Fatal("Failed to extract variables for", cmd)
	}
	result, err := runShellCommand(cmd, isTerminal)
	if expectFail {
		require.Error(t, err, "Expected error in %s command", cmd)
	} else {
		require.NoError(t, err, "Failed to run %s command", cmd)
	}
	require.Equal(t, expanded, sanitize(string(result)), "Unexpected output for %s command", cmd)
}

var (
	timeStampRegexp = regexp.MustCompile(`timestamp: \d+\r?\n`)
	timeRegexp      = regexp.MustCompile(`\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} [\+|-]\d{4} \w{1,3}`)
)

func removeProgramTimestamp(output string) string {
	s := timeStampRegexp.ReplaceAll([]byte(output), []byte("timestamp: <TIMESTAMP>"))
	s = timeRegexp.ReplaceAll(s, []byte("<DATE> <TIME> <TZ>"))
	return string(s)
}
