package nessie

import (
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"os/exec"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

// lakectl tests utility functions
var update = flag.Bool("update", false, "update golden files with results")

func lakectlLocation() string {
	return viper.GetString("lakectl_dir") + "/lakectl"
}

func Lakectl() string {
	lakectlCmdline :=
		"LAKECTL_CREDENTIALS_ACCESS_KEY_ID=" + viper.GetString("access_key_id") +
			" LAKECTL_CREDENTIALS_SECRET_ACCESS_KEY=" + viper.GetString("secret_access_key") +
			" LAKECTL_SERVER_ENDPOINT_URL=" + viper.GetString("endpoint_url") +
			" " + lakectlLocation()

	return lakectlCmdline
}

func runShellCommand(command string, isTerminal bool) ([]byte, error) {
	fmt.Println("Testing '", command, "'")
	var cmd *exec.Cmd

	// Assuming linux. Not sure this is correct
	if isTerminal {
		cmd = exec.Command("/usr/bin/script", "--return", "--quiet", "-c", command, "/dev/null")
	} else {
		cmd = exec.Command("/bin/sh", "-c", command)
	}

	return cmd.CombinedOutput()
}

var varRegexp = regexp.MustCompile(`\$\{([^${}]+)\}`)

var errVariableNotFound = errors.New("variable not found")

// expandVariables receives a string with (possibly) variables in the form of {VAR_NAME}, and
// a var-name -> value mapping. It attempts to replace all occurrences of all variables with
// the corresponding values from the map. If all variables in the string has their mapping
// expandVariables is successful and returns the result string. If at least one variable does
// not have a mapping, expandVariables fails and returns error
func expandVariables(s string, vars map[string]string) (string, error) {
	s = varRegexp.ReplaceAllStringFunc(s, func(varReference string) string {
		if val, ok := vars[varReference[2:len(varReference)-1]]; ok {
			return val
		}
		return varReference
	})

	if missingVar := varRegexp.FindString(s); missingVar != "" {
		return "", fmt.Errorf("%w, %s", errVariableNotFound, missingVar)
	}

	return s, nil
}

var errValueNotUnique = errors.New("value not unique in mapping")

// embedVariables replaces run-specific values from a string with generic, normalized
// variables, that can later be expanded by expandVariables.
// It receives a string that may contain some run-specific data (e.g. repo-name), and
// a mapping of variable names to values. It then replaces all the values found in the original
// string with the corresponding variable name, in the format of {VAR_NAME}. This string can later
// be consumed by expandVariables.
//
// Notes:
// - embedVarialbes will fail if 2 different variables maps to the same value. While this is a possible
// scenario (e.g. a file named 'master' in 'master' branch) it cannot be processed by embedVariables
// - Values are processed from longest to shortest, and so, if certain var value contains another (e.g.
// VAR1 -> "xy", VAR2 -> "xyz"), the longest option will be considered first. As an example, the string
// "wxyz contains xy which is a prefix of xyz" will be embedded as
// "w{VAR2} contains {VAR1} which is a prefix of {VAR2}")
func embedVariables(s string, vars map[string]string) (string, error) {
	revMap := make(map[string]string)
	vals := make([]string, 0, len(vars)) // collecting all vals, which will be used as keys, in order to control iteration order

	for k, v := range vars {
		if _, exist := revMap[v]; exist {
			return "", fmt.Errorf("%w, %s", errValueNotUnique, v)
		}
		revMap[v] = k
		vals = append(vals, v)
	}

	// Sorting the reversed keys (variable values) by descending length in order to handle longer nbames first
	// This will diminish replacing partial names that were used to construct longer names
	sort.Slice(vals, func(i, j int) bool {
		return len(vals[i]) > len(vals[j])
	})

	for _, val := range vals {
		s = strings.ReplaceAll(s, val, "${"+revMap[val]+"}")
	}

	return s, nil
}

func sanitize(output string, vars map[string]string) string {
	s := normalizeProgramTimestamp(output)
	s = normalizeRandomObjectKey(s, vars["STORAGE"])
	s = normalizeCommitID(s)
	return strings.ReplaceAll(s, "\r\n", "\n")
}

func RunCmdAndVerifySuccessWithFile(t *testing.T, cmd string, isTerminal bool, goldenFile string, vars map[string]string) {
	runCmdAndVerifyWithFile(t, cmd, false, isTerminal, goldenFile, vars)
}

func RunCmdAndVerifyFailureWithFile(t *testing.T, cmd string, isTerminal bool, goldenFile string, vars map[string]string) {
	runCmdAndVerifyWithFile(t, cmd, true, isTerminal, goldenFile, vars)
}

func runCmdAndVerifyWithFile(t *testing.T, cmd string, expectFail bool, isTerminal bool, goldenFile string, vars map[string]string) {
	goldenFile = "golden/" + goldenFile + ".golden"

	if *update {
		result, _ := runShellCommand(cmd, isTerminal)
		s, err := embedVariables(string(result), vars)
		require.NoError(t, err, "Variable embed failed - %s", err)
		err = ioutil.WriteFile(goldenFile, []byte(sanitize(s, vars)), 0600)
		require.NoError(t, err, "Failed to write file %s", goldenFile)
		return
	}
	content, err := ioutil.ReadFile(goldenFile)
	if err != nil {
		t.Fatal("Failed to read ", goldenFile, err)
	}
	expected := sanitize(string(content), vars)
	runCmdAndVerifyResult(t, cmd, expectFail, isTerminal, expected, vars)
}

func RunCmdAndVerifySuccess(t *testing.T, cmd string, isTerminal bool, expected string, vars map[string]string) {
	runCmdAndVerifyResult(t, cmd, false, isTerminal, expected, vars)
}

func RunCmdAndVerifyFailure(t *testing.T, cmd string, isTerminal bool, expected string, vars map[string]string) {
	runCmdAndVerifyResult(t, cmd, true, isTerminal, expected, vars)
}

func runCmdAndVerifyResult(t *testing.T, cmd string, expectFail bool, isTerminal bool, expected string, vars map[string]string) {
	expanded, err := expandVariables(expected, vars)
	if err != nil {
		t.Fatal("Failed to extract variables for", cmd)
	}
	result, err := runShellCommand(cmd, isTerminal)
	if expectFail {
		require.Error(t, err, "Expected error in %s command did not occur. Output -%s", cmd, string(result))
	} else {
		require.NoError(t, err, "Failed to run %s command - %s", cmd, string(result))
	}
	require.Equal(t, expanded, sanitize(string(result), vars), "Unexpected output for %s command", cmd)
}

var (
	timeStampRegexp = regexp.MustCompile(`timestamp: \d+\r?\n`)
	timeRegexp      = regexp.MustCompile(`\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} [-+]\d{4} \w{1,3}`)
	commitIDRegExp  = regexp.MustCompile(`[\d|a-f]{64}`)
)

func normalizeProgramTimestamp(output string) string {
	s := timeStampRegexp.ReplaceAll([]byte(output), []byte("timestamp: <TIMESTAMP>"))
	s = timeRegexp.ReplaceAll(s, []byte("<DATE> <TIME> <TZ>"))
	return string(s)
}

func normalizeRandomObjectKey(output string, objectPrefix string) string {
	objectKeyRegExp := regexp.MustCompile(objectPrefix + `/[\d|a-f]{32}`)
	s := objectKeyRegExp.ReplaceAll([]byte(output), []byte(objectPrefix+"/<OBJECT_KEY>"))
	return string(s)
}

func normalizeCommitID(output string) string {
	s := commitIDRegExp.ReplaceAll([]byte(output), []byte("<COMMIT_ID>"))
	return string(s)
}
