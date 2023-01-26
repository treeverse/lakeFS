package esti

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

// lakectl tests utility functions
var update = flag.Bool("update", false, "update golden files with results")

var (
	errValueNotUnique   = errors.New("value not unique in mapping")
	errVariableNotFound = errors.New("variable not found")
)

var (
	reTimestamp       = regexp.MustCompile(`timestamp: \d+\n`)
	reTime            = regexp.MustCompile(`\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} [-+]\d{4} \w{1,3}`)
	reCommitID        = regexp.MustCompile(`[\d|a-f]{64}`)
	reShortCommitID   = regexp.MustCompile(`[\d|a-f]{16}`)
	reChecksum        = regexp.MustCompile(`[\d|a-f]{32}`)
	reEndpoint        = regexp.MustCompile(`https?://\w+(:\d+)?/api/v\d+/`)
	rePhysicalAddress = regexp.MustCompile(`/data/[0-9a-v]{20}/[0-9a-v]{20}`)
	reVariable        = regexp.MustCompile(`\$\{([^${}]+)}`)
	rePreSignURL      = regexp.MustCompile(`https://\S+data\S+`)
)

func lakectlLocation() string {
	return viper.GetString("lakectl_dir") + "/lakectl"
}

func LakectlWithParams(accessKeyID, secretAccessKey, endPointURL string) string {
	lakectlCmdline := "LAKECTL_CREDENTIALS_ACCESS_KEY_ID=" + accessKeyID +
		" LAKECTL_CREDENTIALS_SECRET_ACCESS_KEY=" + secretAccessKey +
		" LAKECTL_SERVER_ENDPOINT_URL=" + endPointURL +
		" " + lakectlLocation()

	return lakectlCmdline
}

func Lakectl() string {
	return LakectlWithParams(viper.GetString("access_key_id"), viper.GetString("secret_access_key"), viper.GetString("endpoint_url"))
}

func runShellCommand(t *testing.T, command string, isTerminal bool) ([]byte, error) {
	t.Logf("Run shell command '%s'", command)
	// Assuming linux. Not sure if this is correct
	cmd := exec.Command("/bin/sh", "-c", command)
	cmd.Env = append(os.Environ(),
		"LAKECTL_INTERACTIVE="+strconv.FormatBool(isTerminal),
	)
	return cmd.CombinedOutput()
}

// expandVariables receives a string with (possibly) variables in the form of {VAR_NAME}, and
// a var-name -> value mapping. It attempts to replace all occurrences of all variables with
// the corresponding values from the map. If all variables in the string has their mapping
// expandVariables is successful and returns the result string. If at least one variable does
// not have a mapping, expandVariables fails and returns error
func expandVariables(s string, vars map[string]string) (string, error) {
	s = reVariable.ReplaceAllStringFunc(s, func(varReference string) string {
		if val, ok := vars[varReference[2:len(varReference)-1]]; ok {
			return val
		}
		return varReference
	})

	if missingVar := reVariable.FindString(s); missingVar != "" {
		return "", fmt.Errorf("%w, %s", errVariableNotFound, missingVar)
	}

	return s, nil
}

// embedVariables replaces run-specific values from a string with generic, normalized
// variables, that can later be expanded by expandVariables.
// It receives a string that may contain some run-specific data (e.g. repo-name), and
// a mapping of variable names to values. It then replaces all the values found in the original
// string with the corresponding variable name, in the format of {VAR_NAME}. This string can later
// be consumed by expandVariables.
//
// Notes:
// - embedVariables will fail if 2 different variables maps to the same value. While this is a possible
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

	// Sorting the reversed keys (variable values) by descending length in order to handle longer names first
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
	// The order of execution below is important as certain expression can contain others
	// and so, should be handled first
	s := strings.ReplaceAll(output, "\r\n", "\n")
	if _, ok := vars["DATE"]; !ok {
		s = normalizeProgramTimestamp(s)
	}
	s = normalizePreSignURL(s)
	s = normalizeRandomObjectKey(s, vars["STORAGE"])
	s = normalizeCommitID(s)
	s = normalizeChecksum(s)
	s = normalizeShortCommitID(s)
	s = normalizeEndpoint(s, vars["LAKEFS_ENDPOINT"])
	return s
}

func RunCmdAndVerifySuccessWithFile(t *testing.T, cmd string, isTerminal bool, goldenFile string, vars map[string]string) {
	t.Helper()
	runCmdAndVerifyWithFile(t, cmd, goldenFile, false, isTerminal, vars)
}

func RunCmdAndVerifyContainsText(t *testing.T, cmd string, isTerminal bool, expectedRaw string, vars map[string]string) {
	t.Helper()
	expected := sanitize(expectedRaw, vars)
	sanitizedResult := runCmd(t, cmd, false, isTerminal, vars)
	require.Contains(t, sanitizedResult, expected)
}

func RunCmdAndVerifyFailureWithFile(t *testing.T, cmd string, isTerminal bool, goldenFile string, vars map[string]string) {
	t.Helper()
	runCmdAndVerifyWithFile(t, cmd, goldenFile, true, isTerminal, vars)
}

func runCmdAndVerifyWithFile(t *testing.T, cmd, goldenFile string, expectFail, isTerminal bool, vars map[string]string) {
	t.Helper()
	goldenFile = "golden/" + goldenFile + ".golden"

	if *update {
		updateGoldenFile(t, cmd, isTerminal, goldenFile, vars)
	} else {
		content, err := os.ReadFile(goldenFile)
		if err != nil {
			t.Fatal("Failed to read ", goldenFile, err)
		}
		expected := sanitize(string(content), vars)
		runCmdAndVerifyResult(t, cmd, expectFail, isTerminal, expected, vars)
	}
}

func updateGoldenFile(t *testing.T, cmd string, isTerminal bool, goldenFile string, vars map[string]string) {
	t.Helper()
	result, _ := runShellCommand(t, cmd, isTerminal)
	s := sanitize(string(result), vars)
	s, err := embedVariables(s, vars)
	require.NoError(t, err, "Variable embed failed - %s", err)
	err = os.WriteFile(goldenFile, []byte(s), 0o600) //nolint: gomnd
	require.NoError(t, err, "Failed to write file %s", goldenFile)
}

func RunCmdAndVerifySuccess(t *testing.T, cmd string, isTerminal bool, expected string, vars map[string]string) {
	t.Helper()
	runCmdAndVerifyResult(t, cmd, false, isTerminal, expected, vars)
}

func RunCmdAndVerifyFailure(t *testing.T, cmd string, isTerminal bool, expected string, vars map[string]string) {
	t.Helper()
	runCmdAndVerifyResult(t, cmd, true, isTerminal, expected, vars)
}

func runCmd(t *testing.T, cmd string, expectFail bool, isTerminal bool, vars map[string]string) string {
	result, err := runShellCommand(t, cmd, isTerminal)
	if expectFail {
		require.Error(t, err, "Expected error in '%s' command did not occur. Output: %s", cmd, string(result))
	} else {
		require.NoError(t, err, "Failed to run '%s' command - %s", cmd, string(result))
	}
	return sanitize(string(result), vars)
}

func runCmdAndVerifyResult(t *testing.T, cmd string, expectFail bool, isTerminal bool, expected string, vars map[string]string) {
	t.Helper()
	expanded, err := expandVariables(expected, vars)
	if err != nil {
		t.Fatal("Failed to extract variables for:", cmd)
	}
	sanitizedResult := runCmd(t, cmd, expectFail, isTerminal, vars)

	require.Equal(t, expanded, sanitizedResult, "Unexpected output for %s command", cmd)
}

func normalizeProgramTimestamp(output string) string {
	s := reTimestamp.ReplaceAllString(output, "timestamp: <TIMESTAMP>")
	return reTime.ReplaceAllString(s, "<DATE> <TIME> <TZ>")
}

func normalizeRandomObjectKey(output string, objectPrefix string) string {
	objectPrefix = strings.TrimPrefix(objectPrefix, "/")
	for _, match := range rePhysicalAddress.FindAllString(output, -1) {
		output = strings.Replace(output, objectPrefix+match, objectPrefix+"/<OBJECT_KEY>", 1)
	}
	return output
}

func normalizeCommitID(output string) string {
	return reCommitID.ReplaceAllString(output, "<COMMIT_ID>")
}

func normalizeShortCommitID(output string) string {
	return reShortCommitID.ReplaceAllString(output, "<COMMIT_ID_16>")
}

func normalizeChecksum(output string) string {
	return reChecksum.ReplaceAllString(output, "<CHECKSUM>")
}

func normalizeEndpoint(output string, endpoint string) string {
	return reEndpoint.ReplaceAllString(output, endpoint)
}

func normalizePreSignURL(output string) string {
	return rePreSignURL.ReplaceAllString(output, "<PRE_SIGN_URL>")
}
